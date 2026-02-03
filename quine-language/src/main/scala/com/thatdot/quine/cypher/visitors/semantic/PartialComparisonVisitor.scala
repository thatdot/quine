package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object PartialComparisonVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_PartialComparisonExpression(
    ctx: CypherParser.OC_PartialComparisonExpressionContext,
  ): List[SemanticToken] =
    ctx.oC_StringListNullPredicateExpression().accept(StringListNullVisitor)
}
