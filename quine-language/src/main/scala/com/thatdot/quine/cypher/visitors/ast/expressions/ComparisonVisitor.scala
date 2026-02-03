package com.thatdot.quine.cypher.visitors.ast.expressions

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{Expression, Source}

object ComparisonVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_ComparisonExpression(ctx: CypherParser.OC_ComparisonExpressionContext): Expression = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    val lhs = ctx.oC_StringListNullPredicateExpression().accept(StringListNullVisitor)

    ctx.oC_PartialComparisonExpression.asScala.toList.foldLeft(lhs) { (lhs, innerCtx) =>
      val (op, rhs) = innerCtx.accept(PartialComparisonVisitor)
      Expression.BinOp(src, op, lhs, rhs, None)
    }
  }
}
