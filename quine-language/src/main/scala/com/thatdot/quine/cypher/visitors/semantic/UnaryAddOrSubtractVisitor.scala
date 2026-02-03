package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.CypherParser.OC_NonArithmeticOperatorExpressionContext
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object UnaryAddOrSubtractVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_UnaryAddOrSubtractExpression(
    ctx: CypherParser.OC_UnaryAddOrSubtractExpressionContext,
  ): List[SemanticToken] =
    ctx.children.asScala.toList.flatMap {
      case childCtx: OC_NonArithmeticOperatorExpressionContext => childCtx.accept(NonArithmeticOperatorVisitor)
      case _ => List.empty[SemanticToken]
    }
}
