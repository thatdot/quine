package com.thatdot.quine.app.v2api.converters

import com.thatdot.quine.app.model.{query => Query}
import com.thatdot.quine.app.v2api.{definitions => Api}

object ApiToQuery {
  def apply(query: Api.CypherQuery): Query.CypherQuery =
    Query.CypherQuery(
      queryText = query.query,
      parameter = query.parameter,
    )
}
