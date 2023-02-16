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

type QueryOutputProps = {
  standingQueryName: string,
  queryOutputName: string,
  queryOutputDetails: any,
  updateStandingQueries: () => void
}

type CreateModalProps = {
  setCreateModalIsOpen: Dispatch<SetStateAction<Boolean>>,
  updateStandingQueries: () => void
}

type DetailsModalProps = {
  setDetailsModalIsOpen: Dispatch<SetStateAction<Boolean>>,
  queryOutputDetails: any
}

const DetailsModal = ({ setDetailsModalIsOpen, queryOutputDetails }: DetailsModalProps) => {

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
      }}>{JSON.stringify(queryOutputDetails, undefined, 2)}</pre>
    </div>
  )
}

const CreateModal = ({ setCreateModalIsOpen, updateStandingQueries }: CreateModalProps) => {
  const [inputs, setInputs] = useState({standingQueryName: "", outputName: "", body: ""})

  const handleChange = (event: { target: { name: any; value: any } }) => {
    const name = event.target.name;
    const value = event.target.value;
    setInputs(values => ({...values, [name]: value}))
  }

  const handleSubmit = (event: { preventDefault: () => void }) => {
    event.preventDefault()
    standingQueryService
      .registerQueryOutput(
        inputs.standingQueryName,
        inputs.outputName,
        JSON.parse(inputs.body))
      .then(() => updateStandingQueries())
  }

  return(
    <div style={ modalStyle }>
      <button style={modalCloseButtonStyle} onClick={() => setCreateModalIsOpen(false)}>X</button>
      <form onSubmit={ handleSubmit } style={ modalFormStyle }>
        <h2>Create New Ingest Stream</h2>
        <label style={ {margin: 'auto'} }>Standing Query Name:
          <input 
            type="text" 
            name="standingQueryName" 
            value={inputs.standingQueryName} 
            onChange={handleChange}
          />
        </label>
        <label style={ {margin: 'auto'} }>Output Name:
        <input 
            type="text" 
            name="outputName" 
            value={inputs.outputName} 
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

const QueryOutputControlBar = ({
  standingQueryName,
  queryOutputName,
  queryOutputDetails,
  updateStandingQueries,
}: QueryOutputProps) => {
  const [detailsModalIsOpen, setDetailsModalIsOpen] = useState<Boolean>(false)

  const cancelOutput = (name: string) => {
    standingQueryService
      .cancelQueryOutput(standingQueryName, queryOutputName)
      .then(() => updateStandingQueries())
  }

  return (
    <div style={ControlBarStyle}>
      <div title={'Standing Query Name'}>{standingQueryName} - {queryOutputName}</div>
      <div title={'Ingest Stream Status'}>{queryOutputDetails.type}</div>
      <button onClick={() => setDetailsModalIsOpen(true)}>Details</button>
      <button onClick={() => cancelOutput(queryOutputName)}>Cancel</button>
      {detailsModalIsOpen &&
      <DetailsModal 
        setDetailsModalIsOpen={setDetailsModalIsOpen}
        queryOutputDetails={queryOutputDetails}
      />}
    </div>
  )
}

export const QueryOutputPortal = () => {
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
      <h2 style={{ height: "5%" }}>Query Output Portal</h2>
      <div style={{ height: "70%" }}>
        {allStandingQueries.map(standingQuery => (
          Object.entries(standingQuery.outputs).map(([key, value]) => (
            <QueryOutputControlBar
              standingQueryName={standingQuery.name}
              queryOutputName={key}
              queryOutputDetails={value}
              updateStandingQueries={updateStandingQueries}
            />
          ))
        ))}
      </div>
      <button 
        style={CreateButtonStyle}
        onClick={() => setCreateModalIsOpen(true)}
      >
        Register Query Output
      </button>
      {createModalIsOpen &&
      <CreateModal 
        setCreateModalIsOpen={setCreateModalIsOpen} 
        updateStandingQueries={updateStandingQueries} 
      />}
    </div>
  )
}
