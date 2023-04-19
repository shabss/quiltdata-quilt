import * as React from 'react'
import renderer from 'react-test-renderer'
import * as M from '@material-ui/core'

import * as style from 'constants/style'

import Code from './'

describe('components/Code', () => {
  it('should render', () => {
    const tree = renderer
      .create(
        <M.MuiThemeProvider theme={style.appTheme}>
          <Code className="B">A</Code>
        </M.MuiThemeProvider>,
      )
      .toJSON()
    expect(tree).toMatchSnapshot()
  })
})
