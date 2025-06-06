package com.thatdot.quine.app.v2api.converters

import com.thatdot.quine.app.model.{query => Query}
import com.thatdot.quine.app.v2api.{definitions => Api}

object QueryToApi {
  def apply(query: Query.CypherQuery): Api.CypherQuery = Api.CypherQuery(
    query = query.queryText,
    parameter = query.parameter,
  )
}
