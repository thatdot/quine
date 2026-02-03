package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object ExpressionVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_Expression(ctx: CypherParser.OC_ExpressionContext): List[SemanticToken] =
    ctx.oC_OrExpression().accept(OrExpressionVisitor)
}
