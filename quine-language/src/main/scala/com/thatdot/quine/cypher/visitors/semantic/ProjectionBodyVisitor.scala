package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.maybeMatchList
import com.thatdot.quine.language.semantic.SemanticToken

object ProjectionBodyVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_ProjectionBody(ctx: CypherParser.OC_ProjectionBodyContext): List[SemanticToken] = {
    val result = for {
      projectionItems <- Option(ctx.oC_ProjectionItems())
      semanticTokens <- maybeMatchList(projectionItems.oC_ProjectionItem(), ProjectionItemVisitor)
    } yield semanticTokens.flatten

    result.getOrElse(Nil)
  }
}
