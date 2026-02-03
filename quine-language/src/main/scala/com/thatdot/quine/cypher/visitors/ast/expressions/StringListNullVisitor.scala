package com.thatdot.quine.cypher.visitors.ast.expressions

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{Expression, Operator, Source}

object StringListNullVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_StringListNullPredicateExpression(
    ctx: CypherParser.OC_StringListNullPredicateExpressionContext,
  ): Expression = {
    val exp = ctx.oC_AddOrSubtractExpression().accept(AddSubtractVisitor)

    (for {
      nullCtx <- Option(ctx.oC_NullPredicateExpression()).filter(!_.isEmpty)
      head <- Option(nullCtx.get(0))
    } yield {
      val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)
      val testExp = Expression.IsNull(src, exp, None)
      if (head.NOT() == null) {
        testExp
      } else {
        Expression.UnaryOp(src, Operator.Not, testExp, None)
      }
    }).getOrElse(exp)
  }
}
