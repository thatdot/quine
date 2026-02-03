package com.thatdot.quine.cypher.visitors.ast.expressions

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.Expression

object ExpressionVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_Expression(ctx: CypherParser.OC_ExpressionContext): Expression =
    ctx.oC_OrExpression().accept(OrExpressionVisitor)
}
