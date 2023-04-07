import asyncio
import base64
import contextlib
import dataclasses
import functools
import hashlib
import math
import traceback
import typing as T

import aiobotocore.config
import aiobotocore.session
from jsonschema import Draft7Validator
import tenacity
import types_aiobotocore_s3.type_defs as T_S3TypeDefs
from types_aiobotocore_s3.client import S3Client

from t4_lambda_shared.utils import get_quilt_logger

logger = get_quilt_logger()

# TODO: get from env
CONCURRENCY = 1000


class S3hashException(Exception):
    def __init__(self, name, context=None):
        super().__init__(name, context)
        self.name = name
        self.context = context

    def asdict(self):
        return {"name": self.name, "context": self.context}


CHECKSUM_TYPE = "QuiltChecksumSHA256"
LEGACY_CHECKSUM_TYPE = "SHA256"


@dataclasses.dataclass
class Checksum:
    value: bytes
    part_count: T.Optional[int]  # None means legacy mode

    @property
    def is_legacy(self):
        return self.part_count is None

    @property
    def type(self):
        return CHECKSUM_TYPE if not self.is_legacy else LEGACY_CHECKSUM_TYPE

    @property
    def value_str(self):
        if self.is_legacy:
            return self.value.hex()

        b64 = base64.b64encode(self.value).decode()
        return f"{b64}-{self.part_count}" if self.part_count else b64

    @classmethod
    def singlepart(cls, value: bytes):
        return cls(value, 0)

    @classmethod
    def multipart(cls, parts: T.Sequence[bytes]):
        return cls(hash_parts(parts), len(parts))

    @classmethod
    def legacy(cls, value: bytes):
        return cls(value, None)

    def __str__(self):
        return f"{self.type}:{self.value_str}"

    def __repr__(self):
        return f"{self.__class__.__name__}({self!s})"

    def asdict(self):
        return {
            "type": self.type,
            "value": self.value_str,
        }


@dataclasses.dataclass
class S3ObjectSource:
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


@dataclasses.dataclass
class S3ObjectDestination:
    bucket: str
    key: str

    @property
    def boto_args(self):
        return {
            "Bucket": self.bucket,
            "Key": self.key,
        }


@dataclasses.dataclass
class MPURef:
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


TMP_KEY = ".quilt/checksum-tmp"

# 8 MiB -- boto3 default: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.TransferConfig
MIN_PART_SIZE = 1024**2 * 8
MAX_PARTS = 10000  # Maximum number of parts per upload supported by S3


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


def get_existing_checksum(src: S3ObjectSource) -> T.Optional[Checksum]:
    # get object attributes and see if part sizes and checksum algo matches our standard
    # return the existing checksum if it's compliant
    # otherwise return None
    return None


def hash_parts(parts: T.Sequence[bytes]) -> bytes:
    return hashlib.sha256(b"".join(parts)).digest()


@dataclasses.dataclass
class PartDef:
    part_number: int
    part_size: T.Optional[int]
    offset: T.Optional[int]

    @property
    def boto_args(self):
        res: T.Dict[str, T.Any] = {
            "PartNumber": self.part_number,
        }
        if self.part_size is not None and self.offset is not None:
            res[
                "CopySourceRange"
            ] = f"bytes={self.offset}-{self.offset + self.part_size - 1}"
        return res


def enumerate_parts(total_size: int, part_size: T.Optional[int]):
    if part_size is None:
        # single-part upload
        yield PartDef(part_number=1, part_size=None, offset=None)
        return

    # multipart upload
    offset = 0
    part_number = 1
    while offset < total_size:
        actual_part_size = min(part_size, total_size - offset)
        yield PartDef(
            part_number=part_number, part_size=actual_part_size, offset=offset
        )
        offset += actual_part_size
        part_number += 1


async def get_writable_location(
    s3: S3Client,
    src: S3ObjectSource,
    dst: T.Optional[S3ObjectDestination],
) -> S3ObjectDestination:
    if dst is None:
        # find a location writable by the current user if it's not provided
        # default to bucket/.quilt/tmp?
        # XXX: consider using service bucket
        dst = S3ObjectDestination(bucket=src.bucket, key=TMP_KEY)
    return dst


async def compute_checksum_remote(
    s3: S3Client,
    src: S3ObjectSource,
    dst: T.Optional[S3ObjectDestination] = None,
) -> T.Tuple[Checksum, MPURef]:
    dst_task = asyncio.create_task(get_writable_location(s3, src, dst))
    src_head_task = asyncio.create_task(s3.head_object(**src.boto_args))

    dst = await dst_task
    # initiate a multipart upload to the given destination
    upload_data = await s3.create_multipart_upload(
        **dst.boto_args,
        ChecksumAlgorithm="SHA256",
    )

    upload_ref = MPURef(bucket=dst.bucket, key=dst.key, id=upload_data["UploadId"])

    # copy (concurrently) all the parts from src with the proper part size / checksum settings
    total_size = (await src_head_task)["ContentLength"]
    part_size = get_part_size(total_size)
    single_part = part_size is None

    @tenacity.retry
    async def _upload_part(part_def: PartDef):
        res = await s3.upload_part_copy(
            **upload_ref.boto_args,
            **part_def.boto_args,
            CopySource=src.boto_args,
        )
        cs = res["CopyPartResult"].get("ChecksumSHA256")  # base64-encoded
        assert cs is not None
        return base64.b64decode(cs)

    uploads = [_upload_part(p) for p in enumerate_parts(total_size, part_size)]
    # retain the checksums for all the parts to compute the final checksum
    checksums: T.List[bytes] = await asyncio.gather(*uploads)

    print("retry stats:", _upload_part.retry.statistics)

    if single_part:
        checksum = Checksum.singlepart(checksums[0])
    else:
        checksum = Checksum.multipart(checksums)

    # return the calculated checksum and the upload reference
    return checksum, upload_ref


async def get_or_compute_checksum_remote(s3: S3Client, loc: S3ObjectSource) -> Checksum:
    checksum = get_existing_checksum(loc)

    if checksum is None:
        checksum, upload_ref = await compute_checksum_remote(s3, loc)

        try:
            await s3.abort_multipart_upload(**upload_ref.boto_args)
        except Exception:
            logger.exception("Error aborting MPU")

        # parts_res = await s3.list_parts(**upload_ref.boto_args)
        # print('MPU Parts:', parts_res["Parts"])
        # parts = [
        #     {
        #         "PartNumber": p["PartNumber"],
        #         "ChecksumSHA256": p["ChecksumSHA256"],
        #         "ETag": p["ETag"],
        #     } for p in parts_res["Parts"]
        # ]
        # print('complete args', dict(
        #     **upload_ref.boto_args,
        #     MultipartUpload={"Parts": parts},
        #     ChecksumSHA256=f"{checksum.value}-{checksum.part_count}",
        # ))
        # res = await s3.complete_multipart_upload(
        #     **upload_ref.boto_args,
        #     MultipartUpload={"Parts": parts},
        #     ChecksumSHA256=checksum.value_str,
        # )

    return checksum


async def compute(loc: S3ObjectSource, legacy: bool) -> Checksum:
    assert user_boto_session is not None
    config = aiobotocore.config.AioConfig(max_pool_connections=CONCURRENCY)
    async with user_boto_session.create_client("s3", config=config) as s3:
        return await get_or_compute_checksum_remote(s3, loc)
    # TODO: support legacy checksums


# Isolated for test-ability.
def get_user_boto_session(aws_access_key_id, aws_secret_access_key, aws_session_token):
    s = aiobotocore.session.get_session()
    s.set_credentials(aws_access_key_id, aws_secret_access_key, aws_session_token)
    return s


user_boto_session: T.Optional[aiobotocore.session.AioSession] = None


@contextlib.contextmanager
def setup_user_boto_session(session: aiobotocore.session.AioSession):
    global user_boto_session
    user_boto_session = session
    try:
        yield user_boto_session
    finally:
        user_boto_session = None


CREDENTIALS_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "id": "https://quiltdata.com/aws-credentials/1",
    "type": "object",
    "properties": {
        "aws_access_key_id": {"type": "string", "pattern": "^.+$"},
        "aws_secret_access_key": {"type": "string", "pattern": "^.+$"},
        "aws_session_token": {"type": "string", "pattern": "^.+$"},
    },
    "required": [
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
    ],
}


def auth(f):
    validator = Draft7Validator(CREDENTIALS_SCHEMA)

    @functools.wraps(f)
    def wrapper(event):
        credentials = event.get("credentials")
        # TODO: collect all errors
        ex = next(validator.iter_errors(credentials), None)
        if ex is not None:
            raise S3hashException("InvalidCredentials", {"details": ex.message})

        with setup_user_boto_session(get_user_boto_session(**credentials)):
            return f(event.get("params"))

    return wrapper


def exception_handler(f):
    @functools.wraps(f)
    def wrapper(event, _context):
        try:
            return {"result": f(event)}
        except S3hashException as e:
            traceback.print_exc()
            return {"error": e.asdict()}

    return wrapper


def get_schema_validator(schema):
    iter_errors = Draft7Validator(schema).iter_errors

    def validator(data):
        ex = next(iter_errors(data), None)
        # TODO: collect all errors
        if ex is not None:
            raise S3hashException("InvalidInputParameters", {"details": ex.message})
        return data

    return validator


def json_api(schema):
    validator = get_schema_validator(schema)

    def innerdec(f):
        @functools.wraps(f)
        def wrapper(params):
            validator(params)
            return f(params)

        return wrapper

    return innerdec


PARAMS_SCHEMA = {
    "type": "object",
    "properties": {
        "location": {
            "type": "object",
            "properties": {
                "bucket": {"type": "string"},
                "key": {"type": "string"},
                "version": {"type": "integer"},
            },
            "required": ["bucket", "key", "version"],
        },
        "legacy": {"type": "boolean"},
    },
    "required": ["location"],
}


# XXX: move decorators to shared?
# XXX: move reusable models/dataclasses to shared?
@exception_handler
@auth
@json_api(PARAMS_SCHEMA)
def lambda_handler(params):
    loc = S3ObjectSource(**params["location"])
    legacy = params.get("legacy", False)
    checksum = asyncio.run(compute(loc, legacy))
    return checksum.asdict()
