package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object StringListNullVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_StringListNullPredicateExpression(
    ctx: CypherParser.OC_StringListNullPredicateExpressionContext,
  ): List[SemanticToken] =
    ctx.oC_AddOrSubtractExpression().accept(AddSubtractVisitor)
}
