package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_QueryContext
import com.thatdot.quine.cypher.utils.Helpers.maybeMatch
import com.thatdot.quine.language.semantic.SemanticToken

object QueryVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_Query(ctx: OC_QueryContext): List[SemanticToken] =
    maybeMatch(ctx.oC_RegularQuery(), RegularQueryVisitor) getOrElse Nil
}
