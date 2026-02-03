package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.CypherParser.OC_UnaryAddOrSubtractExpressionContext
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object PowerOfVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_PowerOfExpression(ctx: CypherParser.OC_PowerOfExpressionContext): List[SemanticToken] =
    ctx.children.asScala.toList.flatMap {
      case childCtx: OC_UnaryAddOrSubtractExpressionContext => childCtx.accept(UnaryAddOrSubtractVisitor)
      case _ => List.empty[SemanticToken]
    }
}
