package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.CypherParser.OC_PowerOfExpressionContext
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object MultiplyDivideModuloVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_MultiplyDivideModuloExpression(
    ctx: CypherParser.OC_MultiplyDivideModuloExpressionContext,
  ): List[SemanticToken] =
    ctx.children.asScala.toList.flatMap {
      case childCtx: OC_PowerOfExpressionContext => childCtx.accept(PowerOfVisitor)
      case _ => List.empty[SemanticToken]
    }
}
