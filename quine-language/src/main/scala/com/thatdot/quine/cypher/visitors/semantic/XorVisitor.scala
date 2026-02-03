package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.CypherParser.OC_AndExpressionContext
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object XorVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_XorExpression(ctx: CypherParser.OC_XorExpressionContext): List[SemanticToken] =
    ctx.children.asScala.toList.flatMap {
      case childCtx: OC_AndExpressionContext => childCtx.accept(AndVisitor)
      case _ => List.empty[SemanticToken]
    }
}
