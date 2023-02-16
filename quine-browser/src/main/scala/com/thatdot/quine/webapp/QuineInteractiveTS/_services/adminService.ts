import { get } from '../_utils/api'

function getQuineConfiguration() {
  const endpoint = 'api/v1/admin/config'
  console.log('getting all ingest streams')
  return get({
    path: endpoint,
    opts: {
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json'
      }
    }
  })
}

export const adminService = {
  getQuineConfiguration
}
