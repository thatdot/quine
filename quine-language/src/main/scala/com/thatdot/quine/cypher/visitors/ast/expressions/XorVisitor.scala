package com.thatdot.quine.cypher.visitors.ast.expressions

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{Expression, Operator, Source, Value}

object XorVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_XorExpression(ctx: CypherParser.OC_XorExpressionContext): Expression = {
    val children = ctx.oC_AndExpression().asScala.toList
    if (children.size == 1) {
      children.head.accept(AndVisitor)
    } else {
      val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

      children.foldRight(Expression.mkAtomicLiteral(Source.NoSource, Value.False))((innerCtx, exp) =>
        Expression.BinOp(src, Operator.Xor, exp, innerCtx.accept(AndVisitor), None),
      )
    }
  }
}
