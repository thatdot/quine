package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object CreateVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_Create(ctx: CypherParser.OC_CreateContext): List[SemanticToken] = {
    val createToken = SemanticToken.fromToken(ctx.start, SemanticType.CreateKeyword)
    val patternTokens = ctx.oC_Pattern().accept(PatternVisitor)

    createToken :: patternTokens
  }
}
