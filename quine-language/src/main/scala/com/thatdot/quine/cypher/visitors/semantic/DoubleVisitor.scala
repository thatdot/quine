package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object DoubleVisitor extends CypherBaseVisitor[SemanticToken] {
  override def visitOC_DoubleLiteral(ctx: CypherParser.OC_DoubleLiteralContext): SemanticToken =
    SemanticToken(
      line = ctx.start.getLine,
      charOnLine = ctx.start.getCharPositionInLine,
      length = (ctx.stop.getStopIndex + 1) - ctx.start.getStartIndex,
      semanticType = SemanticType.DoubleLiteral,
      modifiers = 0,
    )
}
