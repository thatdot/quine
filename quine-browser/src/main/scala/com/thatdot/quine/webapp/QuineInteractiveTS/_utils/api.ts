// const apiUrl = 'http://0.0.0.0:8080'
const apiUrl = ''


/**
 * Handle response from a request (expect json)
 * @private
 *
 * @param {Object} response - Fetch response object
 */
export async function handleResponse(response: { headers: { get: (arg0: string) => any; }; status: any; blob: () => any; json: () => Promise<any>; statusText: any; }) {
  const contentType = response.headers.get('content-type')
  const statusCode = response.status;
  if (statusCode < 400) {
    return response
      .json()
      .catch(() => console.warn('Failed handleResponse()'))
      .then((body) => body)
  }

  return response
    .json()
    .catch(() => console.warn('Failed handleResponse()'))
    .then((body) => {
      throw Error(statusCode, response.statusText)
    })
}

/**
 * Generic request
 *
 * @param {string} path - request path (no leading "/")
 * @param {Object} opts - options passed on to the fetch request
 */
export async function request({ path, opts = {}, rootURL = '' } : {path:string, opts?:object, rootURL?:string}) {
  return fetch(`${rootURL || apiUrl}/${path}`, {
    mode: 'cors',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json'
    },
    ...opts,
  }).then(handleResponse)
}

/**
 * GET request
 *
 * @param {string} path - request path (no leading "/")
 * @param {Object} parameters - request parameters in object form
 * @param {Object} opts - options passed on to the fetch request
 */
export async function get({ path, parameters = {}, opts = {} } : {path:string, parameters?:any, opts?:object}) {
  const search = new URLSearchParams(parameters);
  return request({
    path: `${path}?${search}`,
    opts: {
      method: 'GET',
      ...opts,
    },
  })
}

/**
 * POST request
 *
 * @param {string} path - request path (no leading "/")
 * @param {Object} body - requesty body
 * @param {Object} opts - options passed on to the fetch request
 */
export async function post({ path, body = {}, opts = {} } : {path:string, body?:object, opts?:object}) {
  return request({
    path,
    opts: {
      method: 'POST',
      body: JSON.stringify(body),
      ...opts,
    },
  })
}

/**
 * PUT request
 *
 * @param {string} path - request path (no leading "/")
 * @param {Object} body - requesty body
 * @param {Object} opts - options passed on to the fetch request
 */
export async function put({ path, body = {}, opts = {} } : {path:string, body?:object, opts?:object}) {
  return request({
    path,
    opts: {
      method: 'PUT',
      body: JSON.stringify(body),
      ...opts,
    },
  })
}

/**
 * DELETE request
 *
 * @param {string} path - request path (no leading "/")
 * @param {Object} body - requesty body
 * @param {Object} opts - options passed on to the fetch request
 */
export async function del({ path, body = {}, opts = {} } : {path:string, body?:object, opts?:object}) {
  return request({
    path,
    opts: {
      method: 'DELETE',
      body: JSON.stringify(body),
      ...opts,
    },
  })
}
