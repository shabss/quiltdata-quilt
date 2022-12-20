import * as React from 'react'
import * as urql from 'urql'

import * as Config from 'utils/Config'
import { useIsInStack } from 'utils/BucketConfig'
import { useStatusReportsBucket } from 'utils/StatusReportsBucket'

import * as File from './File'
import * as IFrame from './IFrame'
import * as Text from './Text'
import * as utils from './utils'
import BUCKET_BROWSING_QUERY from './BucketBrowsing.generated'

export const detect = utils.extIn(['.htm', '.html'])

function useDetectBucketBrowsing(bucket) {
  // FIXME: check if bucket config
  //        has `standaloneBrowsing: true` option
  const [{ data }] = urql.useQuery({
    query: BUCKET_BROWSING_QUERY,
    variables: { bucket },
  })
  return data?.bucketConfig?.name === 'fiskus-sandbox-dev'
}

export const Loader = function HtmlLoader({ handle, children }) {
  const bucketBrowsing = useDetectBucketBrowsing(handle.bucket)
  const isInStack = useIsInStack()
  const { mode } = Config.use()
  const statusReportsBucket = useStatusReportsBucket()

  if (bucketBrowsing) {
    return <File.Loader {...{ handle, children }} />
  }

  if (
    mode === 'LOCAL' ||
    isInStack(handle.bucket) ||
    handle.bucket === statusReportsBucket
  ) {
    return <IFrame.Loader {...{ handle, children }} />
  }

  return <Text.Loader {...{ handle, children }} />
}
