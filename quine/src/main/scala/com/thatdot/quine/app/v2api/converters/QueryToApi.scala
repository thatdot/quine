package com.thatdot.quine.app.v2api.converters

import com.thatdot.quine.app.model.{query => Query}
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps.CypherQuery

object QueryToApi {
  def apply(query: Query.CypherQuery): CypherQuery = CypherQuery(
    query = query.queryText,
    parameter = query.parameter,
  )
}
