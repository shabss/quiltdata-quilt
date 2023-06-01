import * as React from 'react'

import type { BucketConfig } from 'components/Form/Package/DestinationBucket'
import { L } from 'components/Form/Package/types'
import { Workflow as WorkflowStruct, notSelected } from 'utils/workflows'

import type { Manifest } from '../../PackageDialog/Manifest'

import useWorkflowsConfig from '../io/workflowsConfig'

export interface WorkflowState {
  errors?: Error[]
  value: WorkflowStruct | null
  workflows: WorkflowStruct[] | typeof L | Error
}

export interface WorkflowContext {
  state: WorkflowState | typeof L
  actions: {
    onChange: (v: WorkflowStruct | null) => void
  }
}

function getDefaultWorkflow(workflows: WorkflowStruct[], manifest?: Manifest) {
  return (
    workflows.find((w) => w.slug === manifest?.workflowId) ||
    workflows.find((w) => w.isDefault) ||
    workflows.find((w) => w.slug === notSelected) ||
    null
  )
}

export default function useWorkflow(
  bucket: BucketConfig | null,
  manifest?: Manifest | typeof L,
): WorkflowContext {
  const [value, setValue] = React.useState<WorkflowStruct | null>(null)
  const [errors, setErrors] = React.useState<Error[] | undefined>()

  const config = useWorkflowsConfig(bucket?.name || null)

  React.useEffect(() => {
    if (value || manifest === L || config === L || config instanceof Error) return
    const defaultWorkflow = getDefaultWorkflow(config.workflows, manifest)
    if (defaultWorkflow) setValue(defaultWorkflow)
  }, [config, manifest, value])

  React.useEffect(() => {
    if (config === L || config instanceof Error) {
      setErrors([new Error('Workflows config is not available')])
      return
    }
    if (config.isWorkflowRequired && (value === null || value.slug === notSelected)) {
      setErrors([new Error('Workflow is required for this bucket')])
      return
    }
    setErrors(undefined)
  }, [config, value])

  const state = React.useMemo(() => {
    if (manifest === L || config === L) return L
    if (config instanceof Error) return { value: null, workflows: config }
    return {
      errors,
      value,
      workflows: config.workflows,
    }
  }, [errors, manifest, value, config])

  return React.useMemo(
    () => ({
      state,
      actions: {
        onChange: setValue,
      },
    }),
    [state],
  )
}
