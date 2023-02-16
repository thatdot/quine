import { get, post, put, del } from '../_utils/api'

function getAllIngestStreams() {
  const endpoint = 'api/v1/ingest'
  console.log('getting all ingest streams')
  return get({
    path: endpoint
  })
}

function createIngestStream(name: string, body:object) {
  const endpoint = `api/v1/ingest/${name}`
  console.log('creating new ingest stream with name: ' + name)
  return post({
    path: endpoint,
    body: body
  })
}

function cancelIngestStream(name: string) {
  const endpoint = `api/v1/ingest/${name}`
  console.log('cancelling ingest stream named: ' + name)
  return del({
    path: endpoint
  })
}

function pauseIngestStream(name: string) {
  const endpoint = `api/v1/ingest/${name}/pause`
  console.log('pausing ingest stream named: ' + name)
  return put({
    path: endpoint
  })
}

function startIngestStream(name: string) {
  const endpoint = `api/v1/ingest/${name}/start`
  console.log('starting ingest stream named: ' + name)
  return put({
    path: endpoint
  })
}

export const ingestStreamService = {
  getAllIngestStreams,
  createIngestStream,
  cancelIngestStream,
  pauseIngestStream,
  startIngestStream
}
