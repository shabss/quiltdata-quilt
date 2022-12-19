import * as React from 'react'
import * as Config from 'utils/Config'
import { useIsInStack } from 'utils/BucketConfig'
import { useStatusReportsBucket } from 'utils/StatusReportsBucket'

import * as IFrameLoader from './IFrame'
import * as Text from './Text'
import * as utils from './utils'

export const detect = utils.extIn(['.htm', '.html'])

export const Loader = function HtmlLoader({ handle, children }) {
  const isInStack = useIsInStack()
  const { mode } = Config.use()
  const statusReportsBucket = useStatusReportsBucket()
  return mode === 'LOCAL' ||
    isInStack(handle.bucket) ||
    handle.bucket === statusReportsBucket ? (
    <IFrameLoader {...{ handle, children }} />
  ) : (
    <Text.Loader {...{ handle, children }} />
  )
}
