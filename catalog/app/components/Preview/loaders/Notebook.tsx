import { dirname, resolve } from 'path'

import createDOMPurify from 'dompurify'
import * as R from 'ramda'
import * as React from 'react'

import type * as Model from 'model'
import * as AWS from 'utils/AWS'
import * as NamedRoutes from 'utils/NamedRoutes'
import * as Resource from 'utils/Resource'
import { PackageHandle } from 'utils/packageHandle'
import { resolveKey } from 'utils/s3paths'
import { useVoila } from 'utils/voila'
import useMemoEq from 'utils/useMemoEq'

import { PreviewData } from '../types'
import FileType from './fileType'
import * as utils from './utils'

const SANITIZE_OPTS = {
  ALLOWED_TAGS: [
    'input',
    'a',
    'abbr',
    'address',
    'article',
    'aside',
    'b',
    'blockquote',
    'caption',
    'code',
    'col',
    'colgroup',
    'dd',
    'details',
    'del',
    'div',
    'dl',
    'dt',
    'em',
    'figure',
    'figcaption',
    'footer',
    'h1',
    'h2',
    'h3',
    'h4',
    'h5',
    'h6',
    'header',
    'hr',
    'i',
    'img',
    'ins',
    'legend',
    'li',
    'mark',
    'nav',
    'ol',
    'p',
    'param',
    'pre',
    'section',
    'span',
    'strong',
    'sub',
    'summary',
    'sup',
    'table',
    'tbody',
    'td',
    'tfoot',
    'th',
    'thead',
    'tr',
    'ul',
  ],
  FORBID_TAGS: ['style', 'script'],
  FORBID_ATTR: ['style'],
}

function useImgProcessor(handle: Model.S3.S3ObjectLocation) {
  const sign = AWS.Signer.useS3Signer()
  return useMemoEq([sign, handle], () =>
    R.pipe(
      Resource.parse,
      Resource.Pointer.case({
        Web: (url) => url,
        S3: ({ bucket, key, version }) =>
          sign({ bucket: bucket || handle.bucket, key, version }),
        S3Rel: (path) =>
          sign({ bucket: handle.bucket, key: resolveKey(handle.key, path) }),
        Path: (path) =>
          sign({ bucket: handle.bucket, key: resolveKey(handle.key, path) }),
      }),
    ),
  )
}

function useLinkProcessor(handle: Model.S3.S3ObjectLocation) {
  const { urls } = NamedRoutes.use()
  const sign = AWS.Signer.useS3Signer()
  return useMemoEq([sign, urls, handle], () =>
    R.pipe(
      Resource.parse,
      Resource.Pointer.case({
        Web: (url) => url,
        S3: ({ bucket, key, version }) =>
          sign({ bucket: bucket || handle.bucket, key, version }),
        S3Rel: (path) =>
          sign({ bucket: handle.bucket, key: resolveKey(handle.key, path) }),
        Path: (p) => {
          const hasSlash = p.endsWith('/')
          const resolved = resolve(dirname(handle.key), p).slice(1)
          const normalized = hasSlash ? `${resolved}/` : resolved
          return hasSlash
            ? urls.bucketDir(handle.bucket, normalized)
            : urls.bucketFile(handle.bucket, normalized)
        },
      }),
    ),
  )
}

type AttributeProcessor = (attr: string) => string

function handleImage(process: AttributeProcessor, element: Element): Element {
  const attributeValue = element.getAttribute('src')
  if (!attributeValue) return element
  const result = process(attributeValue)
  element.setAttribute('src', result)

  const alt = element.getAttribute('alt')
  if (alt) {
    // FIXME: Unescape markdown?
    element.setAttribute('alt', alt)
  }

  return element
}

function handleLink(process: AttributeProcessor, element: HTMLElement): Element {
  const attributeValue = element.getAttribute('href')
  if (typeof attributeValue !== 'string') return element
  const result = process(attributeValue)
  element.setAttribute('href', result)

  const rel = element.getAttribute('rel')
  element.setAttribute('rel', rel ? `${rel} nofollow` : 'nofollow')

  return element
}

function htmlHandler(
  processLink?: AttributeProcessor,
  processImage?: AttributeProcessor,
) {
  return (currentNode: Element): Element => {
    const element = currentNode as HTMLElement
    const tagName = currentNode.tagName?.toUpperCase()
    if (processLink && tagName === 'A') return handleLink(processLink, element)
    if (processImage && tagName === 'IMG') return handleImage(processImage, element)
    return currentNode
  }
}

export const detect = R.pipe(utils.stripCompression, utils.extIs('.ipynb'))

interface PreviewResult {
  html: string
  info: {
    data: {
      head: string[]
      tail: string[]
    }
    note?: string
    warnings?: string
  }
}

interface FileHandle extends Model.S3.S3ObjectLocation {
  packageHandle: PackageHandle
}

interface NotebookLoaderProps {
  children: (result: $TSFixMe) => React.ReactNode
  handle: FileHandle
}

function NotebookLoader({ handle, children }: NotebookLoaderProps) {
  const voilaAvailable = useVoila()
  const data = utils.usePreview({ type: 'ipynb', handle, query: undefined })
  const processLink = useLinkProcessor(handle)
  const processImg = useImgProcessor(handle)
  const processed = utils.useProcessing(data.result, (json: PreviewResult) => {
    const purify = createDOMPurify(window)
    purify.addHook('uponSanitizeElement', htmlHandler(processLink, processImg))
    const preview = purify.sanitize(json.html, SANITIZE_OPTS)
    return PreviewData.Notebook({
      preview,
      note: json.info.note,
      warnings: json.info.warnings,
      modes:
        !!handle.packageHandle && voilaAvailable
          ? [FileType.Jupyter, FileType.Json, FileType.Voila, FileType.Text]
          : [FileType.Jupyter, FileType.Json, FileType.Text],
    })
  })
  return <>{children(utils.useErrorHandling(processed, { handle, retry: data.fetch }))}</>
}

export const Loader = function WrappedNotebookLoader({
  handle,
  children,
}: NotebookLoaderProps) {
  return <NotebookLoader {...{ handle, children }} />
}
