package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.maybeMatch
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object MatchClauseVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_Match(ctx: CypherParser.OC_MatchContext): List[SemanticToken] = {
    val matchToken = ctx.MATCH().getSymbol
    val matchSemanticToken = SemanticToken.fromToken(matchToken, SemanticType.MatchKeyword)

    val patternTokens = ctx.oC_Pattern().accept(PatternVisitor)
    val whereTokens = maybeMatch(ctx.oC_Where(), WhereVisitor).getOrElse(List.empty[SemanticToken])

    matchSemanticToken :: (patternTokens ::: whereTokens)
  }
}
