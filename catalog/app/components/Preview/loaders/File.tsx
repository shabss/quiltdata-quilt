import * as React from 'react'
import * as urql from 'urql'
import * as M from '@material-ui/core'

import AsyncResult from 'utils/AsyncResult'
import type { S3HandleBase } from 'utils/s3paths'
import useQuery from 'utils/useQuery'

import { PreviewData } from '../types'

import BUCKET_BROWSING_QUERY from './BucketBrowsing.generated'

interface Session {
  expiresAt: Date
  id: string
  proxyUrl: string
}

function useSessionCreate(): $TSFixMe {
  return useQuery({
    query: BUCKET_BROWSING_QUERY,
    variables: { bucket: 'fiskus-sandbox-dev' },
  })
}

function useSessionKeepAlive(session: $TSFixMe) {
  const [result, executeQuery] = urql.useQuery({
    query: BUCKET_BROWSING_QUERY,
    variables: { bucket: 'fiskus-sandbox-dev' },
    pause: true,
  })
  React.useEffect(() => {
    if (result.fetching) return
    const ttl = (session.expiresAt.getTime() - Date.now()) / 2
    const timer = setTimeout(() => executeQuery({ requestPolicy: 'network-only' }), ttl)
    return () => clearTimeout(timer)
  }, [executeQuery, result.fetching, session.expiresAt])
}

interface IFrameLoaderProps {
  session: Session
  children: (result: $TSFixMe) => React.ReactNode
  handle: S3HandleBase
}

function IFrameLoader({ children, session }: IFrameLoaderProps) {
  useSessionKeepAlive(session)
  return (
    <>
      {children(
        AsyncResult.Ok(
          PreviewData.IFrame({
            src: session.proxyUrl,
            sandbox: 'allow-scripts allow-same-origin',
          }),
        ),
      )}
    </>
  )
}

interface FileLoaderProps {
  children: (result: $TSFixMe) => React.ReactNode
  handle: S3HandleBase
}

export const Loader = function FileLoader({ handle, children }: FileLoaderProps) {
  const result = useSessionCreate()

  const session = React.useMemo(
    () => ({
      proxyUrl: 'https://quiltdata.com',
      id: 'a8b9e268-7fd9-11ed-b1c4-3c95096f68cb',
      expiresAt: new Date(Date.now() + 5000),
    }),
    [],
  )

  return result.case({
    data: () => <IFrameLoader {...{ handle, children, session }} />,
    _: () => <M.CircularProgress size={48} />,
  })
}
