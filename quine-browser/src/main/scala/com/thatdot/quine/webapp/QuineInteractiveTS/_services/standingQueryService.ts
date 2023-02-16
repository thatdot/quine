import { get, post, del } from '../_utils/api'

function getAllStandingQueries() {
  const endpoint = 'api/v1/query/standing'
  console.log('getting all standing queries')
  return get({
    path: endpoint
  })
}

function createStandingQuery(name: string, body:object) {
  const endpoint = `api/v1/query/standing/${name}`
  console.log('creating new standing query with name: ' + name)
  return post({
    path: endpoint,
    body: body
  })
}

function cancelStandingQuery(name: string) {
  const endpoint = `api/v1/query/standing/${name}`
  console.log('cancelling standing query named: ')
  return del({
    path: endpoint
  })
}

function registerQueryOutput(standingQueryName: string, outputName: string, body: object) {
  const endpoint = `api/v1/query/standing/${standingQueryName}/output/${outputName}`
  console.log(`create output named: ${outputName} for standing query: ${standingQueryName}`)
  return post({
    path: endpoint,
    body: body
  })
}

function cancelQueryOutput(standingQueryName: string, outputName: string) {
  const endpoint = `api/v1/query/standing/${standingQueryName}/output/${outputName}`
  console.log(`cancel output named: ${outputName} for standing query: ${standingQueryName}`)
  return del({
    path: endpoint
  })
}

export const standingQueryService = {
  getAllStandingQueries,
  createStandingQuery,
  cancelStandingQuery,
  registerQueryOutput,
  cancelQueryOutput
}
