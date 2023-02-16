import { useState, useEffect, SetStateAction, Dispatch } from "react"

import { ingestStreamService } from "../_services"
import {
  ControlBarStyle,
  CreateButtonStyle,
  modalStyle,
  modalCloseButtonStyle,
  modalFormStyle,
  ControlBarComponent,
  IngestPortalStyle,
} from "./_componentStyles"
import { useInterval } from "../_hooks/useInterval"

type IngestStreamProps = {
  ingestStreamName: string,
  ingestStreamDetails: any,
  updateIngestStreams: () => void
}

type CreateModalProps = {
  setCreateModalIsOpen: Dispatch<SetStateAction<Boolean>>,
  updateIngestStreams: () => void
}

type DetailsModalProps = {
  setDetailsModalIsOpen: Dispatch<SetStateAction<Boolean>>,
  ingestStreamDetails: any
}

const DetailsModal = ({ setDetailsModalIsOpen, ingestStreamDetails }: DetailsModalProps) => {

  return(
    <div style={ modalStyle }>
      <button style={modalCloseButtonStyle} onClick={() => setDetailsModalIsOpen(false)}>X</button>
      <h2>Ingest Stream Details</h2>
      <pre style={{
        textAlign: 'start',
        overflow: 'auto',
        width: '90%',
        margin: 'auto',
        padding: '1%'
      }}>{JSON.stringify(ingestStreamDetails, undefined, 2)}</pre>
    </div>
  )
}

const CreateModal = ({ setCreateModalIsOpen, updateIngestStreams }: CreateModalProps) => {
  const [inputs, setInputs] = useState({name: "", body: ""})

  const handleChange = (event: { target: { name: any; value: any } }) => {
    const name = event.target.name;
    const value = event.target.value;
    setInputs(values => ({...values, [name]: value}))
  }

  const handleSubmit = (event: { preventDefault: () => void }) => {
    event.preventDefault()
    ingestStreamService
      .createIngestStream(inputs.name, JSON.parse(inputs.body))
      .then(() => updateIngestStreams())
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

const IngestStreamControlBar = ({
  ingestStreamName,
  ingestStreamDetails,
  updateIngestStreams,
}: IngestStreamProps) => {
  const [detailsModalIsOpen, setDetailsModalIsOpen] = useState<Boolean>(false)

  const cancelStream = (name: string) => {
    ingestStreamService
      .cancelIngestStream(name)
      .then(() => updateIngestStreams())
  }

  const startStream = (name: string) => {
    ingestStreamService
      .startIngestStream(name)
      .then(() => updateIngestStreams())
  }

  const pauseStream = (name: string) => {
    ingestStreamService
      .pauseIngestStream(name)
      .then(() => updateIngestStreams())
  }

  return (
    <div style={ControlBarStyle}>
      <div title={'Ingest Stream Name'} style={ControlBarComponent}>
        {ingestStreamName}
      </div>
      <div title={'Ingest Stream Status'} style={ControlBarComponent}>
        {ingestStreamDetails.status}
      </div>
      <div title={'Ingest Stream Count'} style={ControlBarComponent}>
        Count: {ingestStreamDetails.stats.ingestedCount}
      </div>
      <button onClick={() => setDetailsModalIsOpen(true)}>Details</button>
      {ingestStreamDetails.status === "Running" ? (
        <button onClick={() => pauseStream(ingestStreamName)}>Pause</button>
      ) : (
        <button onClick={() => startStream(ingestStreamName)}>Start</button>
      )}
      <button onClick={() => cancelStream(ingestStreamName)}>Cancel</button>
      {detailsModalIsOpen &&
      <DetailsModal 
        setDetailsModalIsOpen={setDetailsModalIsOpen}
        ingestStreamDetails={ingestStreamDetails}
      />}
    </div>
  )
}

export const IngestPortal = () => {
  const [allIngestStreams, setAllIngestStreams] = useState<Object>({})
  const [createModalIsOpen, setCreateModalIsOpen] = useState<Boolean>(false)

  const updateIngestStreams = () => {
    ingestStreamService.getAllIngestStreams().then((jsonResponse) => {
      setAllIngestStreams(jsonResponse)
      console.log(jsonResponse)
    })
  }

  useInterval(() => {
    updateIngestStreams()
  }, 1000 * 2.5)

  useEffect(() => {
    updateIngestStreams()
  }, [])

  return (
    <div style={IngestPortalStyle}>
      <h2 style={{ height: "5%" }}>Ingest Streams</h2>
      <div style={{ height: "70%" }}>
        {Object.entries(allIngestStreams).map(([key, value]) => (
          <IngestStreamControlBar
            key={key}
            ingestStreamName={key}
            ingestStreamDetails={value}
            updateIngestStreams={updateIngestStreams}
          />
        ))}
      </div>
      <button 
        style={CreateButtonStyle}
        onClick={() => setCreateModalIsOpen(true)}
      >
        Create Ingest Stream
      </button>
      {createModalIsOpen &&
      <CreateModal 
        setCreateModalIsOpen={setCreateModalIsOpen} 
        updateIngestStreams={updateIngestStreams} 
      />}
    </div>
  )
}
