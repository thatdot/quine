package com.thatdot.quine.cypher.visitors.ast.expressions

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{Expression, Operator}

object PartialComparisonVisitor extends CypherBaseVisitor[(Operator, Expression)] {
  override def visitOC_PartialComparisonExpression(
    ctx: CypherParser.OC_PartialComparisonExpressionContext,
  ): (Operator, Expression) = {
    val op = ctx.getChild(0).getText.trim match {
      case "=" => Operator.Equals
      case "<>" => Operator.NotEquals
      case "<" => Operator.LessThan
      case "<=" => Operator.LessThanEqual
      case ">" => Operator.GreaterThan
      case ">=" => Operator.GreaterThanEqual
    }
    val exp = ctx.oC_StringListNullPredicateExpression().accept(StringListNullVisitor)

    (op, exp)
  }
}
