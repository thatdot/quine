package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.CypherParser.OC_XorExpressionContext
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object OrExpressionVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_OrExpression(ctx: CypherParser.OC_OrExpressionContext): List[SemanticToken] =
    ctx.children.asScala.toList.flatMap {
      case childCtx: OC_XorExpressionContext => childCtx.accept(XorVisitor)
      case _ => List.empty[SemanticToken]
    }
}
