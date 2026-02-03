package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object WhereVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_Where(ctx: CypherParser.OC_WhereContext): List[SemanticToken] = {
    val whereSemanticToken = SemanticToken.fromToken(ctx.WHERE().getSymbol, SemanticType.WhereKeyword)
    val predicateSemanticTokens = ctx.oC_Expression().accept(ExpressionVisitor)

    whereSemanticToken :: predicateSemanticTokens
  }
}
