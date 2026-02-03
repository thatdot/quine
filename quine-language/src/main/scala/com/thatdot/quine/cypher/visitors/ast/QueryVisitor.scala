package com.thatdot.quine.cypher.visitors.ast

import com.thatdot.quine.cypher.ast.Query
import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_QueryContext
import com.thatdot.quine.cypher.utils.Helpers.maybeMatch

object QueryVisitor extends CypherBaseVisitor[Option[Query]] {
  override def visitOC_Query(ctx: OC_QueryContext): Option[Query] =
    maybeMatch(ctx.oC_RegularQuery(), RegularQueryVisitor).flatten
}
