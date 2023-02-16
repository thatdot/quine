import { useState, useEffect, SetStateAction, Dispatch } from "react"

import { standingQueryService } from "../_services"
import {
  PortalStyle,
  ControlBarStyle,
  CreateButtonStyle,
  modalStyle,
  modalCloseButtonStyle,
  modalFormStyle,
} from "./_componentStyles"

type StandingQueryProps = {
  standingQuery: any,
  updateStandingQueries: () => void
}

type CreateModalProps = {
  setCreateModalIsOpen: Dispatch<SetStateAction<Boolean>>,
  updateStandingQueries: () => void
}

type DetailsModalProps = {
  setDetailsModalIsOpen: Dispatch<SetStateAction<Boolean>>,
  standingQuery: any
}

const DetailsModal = ({ setDetailsModalIsOpen, standingQuery }: DetailsModalProps) => {

  return(
    <div style={ modalStyle }>
      <button style={modalCloseButtonStyle} onClick={() => setDetailsModalIsOpen(false)}>X</button>
      <h2>Standing Query Details</h2>
      <pre style={{
        textAlign: 'start',
        overflow: 'auto',
        width: '90%',
        margin: 'auto',
        padding: '1%'
      }}>{JSON.stringify(standingQuery, undefined, 2)}</pre>
    </div>
  )
}

const CreateModal = ({ setCreateModalIsOpen, updateStandingQueries }: CreateModalProps) => {
  const [inputs, setInputs] = useState({name: "", body: ""})

  const handleChange = (event: { target: { name: any; value: any } }) => {
    const name = event.target.name;
    const value = event.target.value;
    setInputs(values => ({...values, [name]: value}))
  }

  const handleSubmit = (event: { preventDefault: () => void }) => {
    event.preventDefault();
    standingQueryService
      .createStandingQuery(inputs.name, JSON.parse(inputs.body))
      .then(() => updateStandingQueries())
  }

  return(
    <div style={ modalStyle }>
      <button style={modalCloseButtonStyle} onClick={() => setCreateModalIsOpen(false)}>X</button>
      <form onSubmit={ handleSubmit } style={ modalFormStyle }>
        <h2>Create New Ingest Stream</h2>
        <label style={ {margin: 'auto'} }>Name:
          <input 
            type="text" 
            name="name" 
            value={inputs.name} 
            onChange={handleChange}
          />
        </label>
        <label style={ {margin: 'auto'} }>Body:
          <textarea 
            name="body" 
            value={inputs.body} 
            onChange={handleChange}
            style={ {height: '200%'} }
          />
        </label>
        <input style={ {width:'50%', margin: 'auto'} } type="submit" />
      </form>
    </div>
  )
}

const StandingQueryControlBar = ({
  standingQuery,
  updateStandingQueries,
}: StandingQueryProps) => {
  const [detailsModalIsOpen, setDetailsModalIsOpen] = useState<Boolean>(false)

  const cancelQuery = (name: string) => {
    standingQueryService
      .cancelStandingQuery(name)
      .then(() => updateStandingQueries())
  }

  return (
    <div style={ControlBarStyle}>
      <div title={'Standing Query Name'}>{standingQuery.name}</div>
      <div title={'Standing Query Mode'}>{standingQuery.pattern.mode}</div>
      <div title={'Standing Query Type'}>{standingQuery.pattern.type}</div>
      <button onClick={() => setDetailsModalIsOpen(true)}>Details</button>
      <button onClick={() => cancelQuery(standingQuery.name)}>Cancel</button>
      {detailsModalIsOpen &&
      <DetailsModal 
        setDetailsModalIsOpen={setDetailsModalIsOpen}
        standingQuery={standingQuery}
      />}
    </div>
  )
}

export const StandingQueryPortal = () => {
  const [allStandingQueries, setAllStandingQueries] = useState<any[]>([])
  const [createModalIsOpen, setCreateModalIsOpen] = useState<Boolean>(false)

  const updateStandingQueries = () => {
    standingQueryService.getAllStandingQueries().then((jsonResponse) => {
      setAllStandingQueries(jsonResponse)
      console.log(jsonResponse)
    })
  }

  useEffect(() => {
    updateStandingQueries()
  }, [])

  return (
    <div style={PortalStyle}>
      <h2 style={{ height: "5%" }}>Standing Queries</h2>
      <div style={{ height: "70%" }}>
        {allStandingQueries.map(standingQuery => (
          <StandingQueryControlBar
            key={standingQuery.name}
            standingQuery={standingQuery}
            updateStandingQueries={updateStandingQueries}
          />
        ))}
      </div>
      <button 
        style={CreateButtonStyle}
        onClick={() => setCreateModalIsOpen(true)}
      >
        Create Standing Query
      </button>
      {createModalIsOpen &&
      <CreateModal 
        setCreateModalIsOpen={setCreateModalIsOpen} 
        updateStandingQueries={updateStandingQueries} 
      />}
    </div>
  )
}
