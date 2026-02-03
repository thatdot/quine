package com.thatdot.quine.cypher.visitors.ast.expressions

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{Expression, Operator}

object AndVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_AndExpression(ctx: CypherParser.OC_AndExpressionContext): Expression = {
    val children = ctx.oC_NotExpression().asScala.toList
    val first = children.head.oC_ComparisonExpression().accept(ComparisonVisitor)
    children.tail.foldLeft(first) { (exp, innerCtx) =>
      val subExp = innerCtx.oC_ComparisonExpression().accept(ComparisonVisitor)
      Expression.BinOp(
        source = subExp.source,
        op = Operator.And,
        lhs = exp,
        rhs = subExp,
        None,
      )
    }
  }
}
