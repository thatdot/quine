package com.thatdot.quine.app.v2api.converters

import com.thatdot.quine.app.model.{query => Query}
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps.CypherQuery

object ApiToQuery {
  def apply(query: CypherQuery): Query.CypherQuery =
    Query.CypherQuery(
      queryText = query.query,
      parameter = query.parameter,
    )
}
