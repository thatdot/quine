package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object PatternElementChainVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_PatternElementChain(ctx: CypherParser.OC_PatternElementChainContext): List[SemanticToken] = {
    val relationTokens = ctx.oC_RelationshipPattern().accept(RelationshipPatternVisitor)
    val destinationTokens = ctx.oC_NodePattern().accept(NodePatternVisitor)

    relationTokens ::: destinationTokens
  }
}
