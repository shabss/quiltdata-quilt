from __future__ import annotations

import asyncio
import base64
import contextlib
import contextvars
import enum
import functools
import hashlib
import math
import os
import typing as T

import aiobotocore.config
import aiobotocore.response
import aiobotocore.session
import botocore.exceptions
import pydantic
import tenacity
import types_aiobotocore_s3.type_defs as T_S3TypeDefs
from types_aiobotocore_s3.client import S3Client

from t4_lambda_shared.utils import get_quilt_logger

logger = get_quilt_logger()

DEFAULT_CONCURRENCY = int(os.environ["MPU_CONCURRENCY"])


S3: contextvars.ContextVar[S3Client] = contextvars.ContextVar("s3")


NonEmptyStr = pydantic.constr(min_length=1, strip_whitespace=True)


class AWSCredentials(pydantic.BaseModel):
    key: NonEmptyStr
    secret: NonEmptyStr
    token: NonEmptyStr


# Isolated for test-ability.
def get_user_boto_session(
    credentials: AWSCredentials,
) -> aiobotocore.session.AioSession:
    s = aiobotocore.session.get_session()
    s.set_credentials(credentials.key, credentials.secret, credentials.token)
    return s


@contextlib.asynccontextmanager
async def aio_context(credentials: AWSCredentials, concurrency: pydantic.PositiveInt):
    session = get_user_boto_session(credentials)
    config = aiobotocore.config.AioConfig(max_pool_connections=concurrency)

    async with session.create_client("s3", config=config) as s3:
        s3_token = S3.set(s3)
        yield
        S3.reset(s3_token)


class S3hashException(Exception):
    def __init__(self, name, context=None):
        super().__init__(name, context)
        self.name = name
        self.context = context

    def dict(self):
        return {"name": self.name, "context": self.context}


class ChecksumType(str, enum.Enum):
    MP = "QuiltMultipartSHA256"
    SP = "SHA256"


class Checksum(pydantic.BaseModel):
    type: ChecksumType
    value: str

    @classmethod
    def singlepart(cls, value: bytes):
        return cls(value=value.hex(), type=ChecksumType.SP)

    @classmethod
    def multipart(cls, parts: T.Sequence[bytes]):
        hash_list = hash_parts(parts)
        b64 = base64.b64encode(hash_list).decode()
        value = f"{b64}-{len(parts)}"
        return cls(value=value, type=ChecksumType.MP)

    @classmethod
    def for_parts(cls, checksums: T.Sequence[bytes], defs: T.Sequence[PartDef]):
        if defs == PARTS_SINGLE:
            return cls.singlepart(checksums[0])
        return cls.multipart(checksums)

    def __str__(self):
        return f"{self.type}:{self.value}"

    def __repr__(self):
        return f"{self.__class__.__name__}({self!s})"


class S3ObjectSource(pydantic.BaseModel):
    bucket: str
    key: str
    version: str

    @property
    def boto_args(self) -> T_S3TypeDefs.CopySourceTypeDef:
        return {
            "Bucket": self.bucket,
            "Key": self.key,
            "VersionId": self.version,
        }


class S3ObjectDestination(pydantic.BaseModel):
    bucket: str
    key: str

    @property
    def boto_args(self):
        return {
            "Bucket": self.bucket,
            "Key": self.key,
        }


class MPURef(pydantic.BaseModel):
    bucket: str
    key: str
    id: str

    @property
    def boto_args(self):
        return {
            "Bucket": self.bucket,
            "Key": self.key,
            "UploadId": self.id,
        }


# 8 MiB -- boto3 default: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.TransferConfig
MIN_PART_SIZE = 1024**2 * 8
MAX_PARTS = 10000  # Maximum number of parts per upload supported by S3


# XXX: import this logic from quilt3 when it's available
def get_part_size(file_size: int) -> T.Optional[int]:
    if file_size < MIN_PART_SIZE:
        return None  # use single-part upload (and plain SHA256 hash)

    # NOTE: in the case where file_size is exactly equal to MIN_PART_SIZE,
    # boto creates a 1-part multipart upload :shrug:
    part_size = MIN_PART_SIZE
    num_parts = math.ceil(file_size / part_size)

    while num_parts > MAX_PARTS:
        part_size *= 2
        num_parts = math.ceil(file_size / part_size)

    return part_size


async def get_existing_checksum(
    location: S3ObjectSource,
    parts: T.Sequence[PartDef],
) -> T.Optional[Checksum]:
    # XXX
    # get object attributes and see if part sizes and checksum algo matches our standard
    # return the existing checksum if it's compliant
    # otherwise return None
    return None


def hash_parts(parts: T.Sequence[bytes]) -> bytes:
    return hashlib.sha256(b"".join(parts)).digest()


class PartDef(pydantic.BaseModel):
    part_number: int
    range: T.Optional[T.Tuple[int, int]]

    @property
    def boto_args(self):
        args: T.Dict[str, T.Any] = {"PartNumber": self.part_number}
        if self.range:
            args["CopySourceRange"] = f"bytes={self.range[0]}-{self.range[1]}"
        return args

    @classmethod
    def for_range(cls, part_number: int, offset: int, size: int):
        return cls(
            part_number=part_number,
            range=(offset, offset + size - 1),
        )


PARTS_SINGLE = [PartDef(part_number=1, range=None)]


async def get_parts_for_location(location: S3ObjectSource) -> T.List[PartDef]:
    total_size = (await S3.get().head_object(**location.boto_args))["ContentLength"]
    part_size = get_part_size(total_size)

    # single-part upload
    if part_size is None:
        return PARTS_SINGLE

    # multipart upload
    offset = 0
    part_number = 1
    parts = []
    while offset < total_size:
        actual_part_size = min(part_size, total_size - offset)
        parts.append(
            PartDef.for_range(
                part_number=part_number,
                offset=offset,
                size=actual_part_size,
            )
        )
        offset += actual_part_size
        part_number += 1

    return parts


@tenacity.retry
async def upload_part(
    mpu: MPURef,
    src: S3ObjectSource,
    part: PartDef,
) -> bytes:
    res = await S3.get().upload_part_copy(
        **mpu.boto_args,
        **part.boto_args,
        CopySource=src.boto_args,
    )
    cs = res["CopyPartResult"].get("ChecksumSHA256")  # base64-encoded
    assert cs is not None
    return base64.b64decode(cs)


async def compute_part_checksums(
    mpu: MPURef,
    location: S3ObjectSource,
    parts: T.Sequence[PartDef],
) -> T.Tuple[T.List[bytes], T.Any]:
    uploads = [upload_part(mpu, location, p) for p in parts]
    checksums: T.List[bytes] = await asyncio.gather(*uploads)
    return checksums, upload_part.retry.statistics


async def _create_mpu(target: S3ObjectDestination) -> MPURef:
    upload_data = await S3.get().create_multipart_upload(
        **target.boto_args,
        ChecksumAlgorithm="SHA256",
    )
    return MPURef(bucket=target.bucket, key=target.key, id=upload_data["UploadId"])


MPUDstError = T.Tuple[S3ObjectDestination, botocore.exceptions.ClientError]

MPUTarget = T.Union[S3ObjectDestination, T.List[S3ObjectDestination]]


async def create_mpu(target: MPUTarget) -> MPURef:
    dsts = target if isinstance(target, list) else [target]

    errors: T.List[MPUDstError] = []

    for dst in dsts:
        try:
            return await _create_mpu(dst)
        except botocore.exceptions.ClientError as ex:
            errors.append((dst, ex))

    raise S3hashException(
        "MPUError",
        {
            "errors": [
                {
                    "dst": dst.dict(),
                    "error": str(ex),
                }
                for dst, ex in errors
            ]
        },
    )


STREAM_CHUNK_SIZE = 128 * 2**10


async def compute_checksum_stream(location: S3ObjectSource) -> Checksum:
    logger.info("compute_checksums_stream")
    resp = await S3.get().get_object(**location.boto_args)
    async with resp["Body"] as stream:
        stream: aiobotocore.response.StreamingBody
        hashobj = hashlib.sha256()
        async for chunk in stream.iter_chunks(chunk_size=STREAM_CHUNK_SIZE):
            hashobj.update(chunk)
        return Checksum.singlepart(hashobj.digest())


class ChecksumResult(pydantic.BaseModel):
    checksum: Checksum
    stats: T.Optional[dict] = None


# XXX: need a consistent way to serialize / deserialize exceptions
def lambda_wrapper(f):
    @functools.wraps(f)
    def wrapper(event, context):
        # XXX: make sure to disable in production to avoid leaking credentials
        logger.info("event: %s", event)
        logger.info("context: %s", context)
        try:
            result = asyncio.run(f(**event))
            logger.info("result: %s", result)
            return {"result": result.dict()}
        except S3hashException as e:
            logger.exception("S3hashException")
            return {"error": e.dict()}
        except pydantic.ValidationError as e:
            # XXX: make it .info()?
            logger.exception("ValidationError")
            # TODO: expose advanced pydantic error reporting capabilities
            return {
                "error": {
                    "name": "InvalidInputParameters",
                    "context": {"details": str(e)},
                },
            }

    return wrapper


# XXX: move decorators to shared?
# XXX: move reusable types/models to shared?
@lambda_wrapper
@pydantic.validate_arguments
async def lambda_handler(
    *,
    credentials: AWSCredentials,
    location: S3ObjectSource,
    multipart: bool,
    target: T.Optional[MPUTarget] = None,
    concurrency: T.Optional[pydantic.PositiveInt] = None,
) -> ChecksumResult:
    if concurrency is None:
        concurrency = DEFAULT_CONCURRENCY

    logger.info(
        "arguments: %s",
        {
            "location": location,
            "multipart": multipart,
            "target": target,
            "concurrency": concurrency,
        },
    )

    async with aio_context(credentials, concurrency):
        if not multipart and target is None:
            checksum = await compute_checksum_stream(location)
            return ChecksumResult(checksum=checksum)

        if target is None or isinstance(target, list) and len(target) == 0:
            raise S3hashException(
                "InvalidInputParameters",
                {"details": "target must be specified for multipart checksums"},
            )

        part_defs = (
            await get_parts_for_location(location) if multipart else PARTS_SINGLE
        )

        logger.info("parts: %s", len(part_defs))

        checksum = await get_existing_checksum(location, part_defs)
        if checksum is not None:
            logger.info("got existing checksum")
            return ChecksumResult(checksum=checksum)

        mpu = await create_mpu(target)

        logger.info("MPU created: %s", mpu)

        part_checksums, retry_stats = await compute_part_checksums(
            mpu,
            location,
            part_defs,
        )

        logger.info("got checksums. retry stats: %s", retry_stats)

        checksum = Checksum.for_parts(part_checksums, part_defs)

        try:
            await S3.get().abort_multipart_upload(**mpu.boto_args)
        except Exception:
            # XXX: send to sentry
            logger.exception("Error aborting MPU")

        # TODO: expose perf (timing) stats
        stats = {"retry": retry_stats}
        return ChecksumResult(checksum=checksum, stats=stats)
