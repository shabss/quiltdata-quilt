import * as React from 'react'

import 'utils/perspective-pollution'

import perspective from '@finos/perspective'
import type { Table } from '@finos/perspective'
import type { HTMLPerspectiveViewerElement } from '@finos/perspective-viewer'

const worker = perspective.worker()

export function renderViewer(
  parentNode: HTMLElement,
  { className }: React.HTMLAttributes<HTMLDivElement>,
): HTMLPerspectiveViewerElement {
  const element = document.createElement('perspective-viewer')
  if (className) {
    element.className = className
  }
  parentNode.appendChild(element)
  return element
}

export async function renderTable(
  data: string | ArrayBuffer,
  viewer: HTMLPerspectiveViewerElement,
) {
  const table = await worker.table(data)
  viewer.load(table)
  return table
}

function usePerspective(
  container: HTMLDivElement | null,
  data: string | ArrayBuffer,
  attrs: React.HTMLAttributes<HTMLDivElement>,
) {
  React.useEffect(() => {
    let table: Table, viewer: HTMLPerspectiveViewerElement

    async function fetchData() {
      if (!container) return

      viewer = renderViewer(container, attrs)
      table = await renderTable(data, viewer)
    }
    fetchData()

    async function disposeTable() {
      viewer?.parentNode?.removeChild(viewer)
      await viewer?.delete()
      await table?.delete()
    }

    return () => {
      disposeTable()
    }
  }, [attrs, container, data])
}

export const use = usePerspective
