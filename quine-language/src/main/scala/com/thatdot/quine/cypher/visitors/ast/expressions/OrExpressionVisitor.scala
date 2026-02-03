package com.thatdot.quine.cypher.visitors.ast.expressions

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{Expression, Operator, Source, Value}

object OrExpressionVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_OrExpression(ctx: CypherParser.OC_OrExpressionContext): Expression = {
    val children = ctx.oC_XorExpression().asScala.toList
    if (children.size == 1) {
      children.head.accept(XorVisitor)
    } else {
      val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)
      children.foldRight(Expression.mkAtomicLiteral(Source.NoSource, Value.False))((innerCtx, exp) =>
        Expression.BinOp(src, Operator.Or, exp, innerCtx.accept(XorVisitor), None),
      )
    }
  }
}
