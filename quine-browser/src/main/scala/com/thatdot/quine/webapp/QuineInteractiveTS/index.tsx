import React from 'react'
import { atom, useAtom } from 'jotai'
import useWebSocket from 'react-use-websocket'

import {
  ConfigurationPortal,
  IngestPortal,
  QueryOutputPortal,
  StandingQueryPortal
} from './_components'
import { PortalContainerStyle } from './_components/_componentStyles'

const ClientStyle = {
  backgroundColor: 'white',
  width: '100vw',
  height: '100vh',
  display: 'flex',
  'flex-wrap': 'wrap',
  'flex-direction': 'row',
  justifyContent: 'space-around',
  'padding-left': '.5%'
}

const headerStyle = {
  backgroundColor: 'white',
  width: '100%',
  height: '5%',
  margin: 'auto',
}

const WS_URL = "ws://0.0.0.0:8080/api/v1/query"
const queryAtom = atom({})

export const InteractiveClient = () => {
  const [query, setQuery] = useAtom(queryAtom) 

  useWebSocket(WS_URL, {
    onOpen: () => {
      console.log('WebSocket connection established.');
    },
    onMessage: (message: { data: any }) => {
      console.log(message.data)
    }
  });

  return (
    <div style={ClientStyle}>
      <h1 style={ headerStyle }>Quine Interactive</h1>
      <IngestPortal />
      <div style={PortalContainerStyle}>
        <ConfigurationPortal />
      </div>
      <div style={PortalContainerStyle}>
        <StandingQueryPortal />
        <QueryOutputPortal />
      </div>
    </div>
  )
}

