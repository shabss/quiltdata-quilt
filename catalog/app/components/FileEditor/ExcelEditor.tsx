import * as React from 'react'
import * as xlsx from 'xlsx'
import * as M from '@material-ui/core'
import * as Lab from '@material-ui/lab'

import Lock from 'components/Lock'
import Perspective from 'components/Preview/renderers/Perspective'

const useStyles = M.makeStyles((t) => ({
  root: {
    position: 'relative',
    width: '100%',
  },
  error: {
    marginBottom: t.spacing(1),
  },
}))

export interface ExcelEditorProps {
  disabled?: boolean
  onChange: (value: string) => void
  initialValue?: Uint8Array | string
  error: Error | null
}

export default function ExcelEditor({
  disabled,
  error,
  initialValue,
  onChange,
}: ExcelEditorProps) {
  const classes = useStyles()
  const [data] = React.useState(() => {
    const wb = xlsx.read(initialValue)
    const ws = wb.Sheets[wb.SheetNames[0]]
    return xlsx.utils.sheet_to_csv(ws)
  })
  const config = React.useMemo(() => ({ plugin_config: { editable: true } }), [])
  const handleRender = React.useCallback(
    async (d) => {
      const t = await d.parentNode?.parentNode?.getTable()
      if (!t) return
      const view = await t?.view()
      const aoa = await view?.to_json()
      const ws = xlsx.utils.json_to_sheet(aoa)
      var workbook = xlsx.utils.book_new()
      xlsx.utils.book_append_sheet(workbook, ws)
      await view?.delete()
      onChange(xlsx.write(workbook, { type: 'buffer' }))
    },
    [onChange],
  )
  return (
    <div className={classes.root}>
      {error && (
        <Lab.Alert severity="error" className={classes.error} variant="outlined">
          {error.message}
        </Lab.Alert>
      )}
      <Perspective
        data={data}
        truncated={false}
        config={config}
        onRender={handleRender}
      />
      {disabled && <Lock />}
    </div>
  )
}
