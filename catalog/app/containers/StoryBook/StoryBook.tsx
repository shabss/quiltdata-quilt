import * as React from 'react'
import * as RRDom from 'react-router-dom'
import * as M from '@material-ui/core'

import Layout from 'components/Layout'

import JsonEditorBasic from './JsonEditor/Basic'
import JsonEditorHasInitialValue from './JsonEditor/HasInitialValue'

const books = [
  {
    path: '/jsoneditor',
    title: 'JsonEditor',
    children: [
      {
        Component: JsonEditorBasic,
        path: '/basic',
        title: 'Basic',
      },
      {
        Component: JsonEditorHasInitialValue,
        path: '/initial-value',
        title: 'Has initial value',
      },
    ],
  },
]

const useStyles = M.makeStyles((t) => ({
  root: {
    display: 'flex',
    flexGrow: 1,
  },
  sidebar: {
    whiteSpace: 'nowrap',
    boxShadow: `inset 0px 2px 1px -1px rgba(0,0,0,0.2),
                inset 0px 1px 1px 0px rgba(0,0,0,0.14),
                inset 0px 1px 3px 0px rgba(0,0,0,0.12)`,
  },
  menu: {},
  subMenu: {
    paddingLeft: t.spacing(2),
  },
  content: {
    paddingTop: t.spacing(2),
    background: t.palette.common.white,
  },
}))

function StoryBook() {
  const classes = useStyles()
  const { path, url } = RRDom.useRouteMatch()
  const [subMenu, setSubMenu] = React.useState('')
  return (
    <div className={classes.root}>
      <div className={classes.sidebar}>
        <M.List className={classes.menu}>
          {books.map((group) => (
            <>
              <M.ListItem onClick={() => setSubMenu(group.path)}>
                <M.ListItemText>{group.title}</M.ListItemText>
                <M.Icon>{subMenu === group.path ? 'expand_less' : 'expand_more'}</M.Icon>
              </M.ListItem>
              <M.Collapse in={subMenu === group.path}>
                <M.List className={classes.subMenu} dense disablePadding>
                  {group.children.map((book) => (
                    <M.ListItem>
                      <RRDom.Link to={`${url}${group.path}${book.path}`}>
                        {book.title}
                      </RRDom.Link>
                    </M.ListItem>
                  ))}
                </M.List>
              </M.Collapse>
            </>
          ))}
        </M.List>
      </div>
      <M.Container maxWidth="lg" className={classes.content}>
        <RRDom.Switch>
          {books.map((group) =>
            group.children.map((book) => (
              <RRDom.Route
                path={`${path}${group.path}${book.path}`}
                component={book.Component}
              />
            )),
          )}
        </RRDom.Switch>
      </M.Container>
    </div>
  )
}

export default function StoryBookPage() {
  return <Layout pre={<StoryBook />} />
}
