package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.CypherParser.OC_ComparisonExpressionContext
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object NotVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_NotExpression(ctx: CypherParser.OC_NotExpressionContext): List[SemanticToken] =
    ctx.children.asScala.toList.flatMap {
      case childCtx: OC_ComparisonExpressionContext => childCtx.accept(ComparisonVisitor)
      case _ => List.empty[SemanticToken]
    }
}
