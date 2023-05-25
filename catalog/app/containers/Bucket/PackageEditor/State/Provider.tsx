import * as React from 'react'

import useBucket, { BucketContext } from './Bucket'
import useManifest from './Manifest'
import useMessage, { MessageContext } from './Message'
import useMeta, { MetaContext } from './Meta'
import useName, { NameContext } from './Name'
import useSource from './Source'
import useWorkflow, { WorkflowContext } from './Workflow'

interface ContextData {
  bucket: BucketContext
  message: MessageContext
  meta: MetaContext
  name: NameContext
  workflow: WorkflowContext
}

const Ctx = React.createContext<ContextData | null>(null)

interface ProviderProps {
  bucket: string
  name: string
  hashOrTag: string
  hash?: string
  path: string
  mode?: string
  resolvedFrom?: string
  size?: number
  children: React.ReactNode
}

export default function Provider({
  bucket: srcBucket,
  name: srcName,
  hashOrTag,
  path,
  children,
}: ProviderProps) {
  const src = useSource(srcBucket, srcName, hashOrTag, path)
  const bucket = useBucket(src)

  const manifest = useManifest(src)
  const workflow = useWorkflow(bucket.state?.value, manifest)
  const name = useName(src, workflow)
  const message = useMessage()
  const meta = useMeta(workflow, manifest)
  const v = React.useMemo(
    () => ({
      bucket,
      message,
      name,
      workflow,
      meta,
    }),
    [bucket, message, meta, name, workflow],
  )
  return <Ctx.Provider value={v}>{children}</Ctx.Provider>
}

export function useContext(): ContextData {
  const data = React.useContext(Ctx)
  if (!data) throw new Error('Set provider')
  return data
}

export const use = useContext
