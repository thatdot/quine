package com.thatdot.quine.cypher.visitors.ast.expressions

import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.antlr.v4.runtime.tree.TerminalNode

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{Expression, Operator, Source}

object UnaryAddSubtractVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_UnaryAddOrSubtractExpression(
    ctx: CypherParser.OC_UnaryAddOrSubtractExpressionContext,
  ): Expression = {
    val children = ctx.children.asScala
    val maybeSign: Option[Operator] = children.head match {
      case node: TerminalNode =>
        node.getText.trim match {
          case "+" => Some(Operator.Plus)
          case "-" => Some(Operator.Minus)
        }
      case _ => None
    }
    val exp = ctx.oC_NonArithmeticOperatorExpression().accept(NonArithmeticOperatorVisitor)

    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    maybeSign match {
      case Some(sign) => Expression.UnaryOp(src, sign, exp, None)
      case None => exp
    }
  }
}
