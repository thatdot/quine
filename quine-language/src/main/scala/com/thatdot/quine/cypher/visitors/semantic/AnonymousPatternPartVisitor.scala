package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object AnonymousPatternPartVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_AnonymousPatternPart(ctx: CypherParser.OC_AnonymousPatternPartContext): List[SemanticToken] =
    ctx.oC_PatternElement().accept(PatternElementVisitor)
}
