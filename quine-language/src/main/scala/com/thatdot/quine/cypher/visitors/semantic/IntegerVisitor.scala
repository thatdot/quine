package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object IntegerVisitor extends CypherBaseVisitor[SemanticToken] {
  override def visitOC_IntegerLiteral(ctx: CypherParser.OC_IntegerLiteralContext): SemanticToken =
    SemanticToken.fromToken(ctx.DecimalInteger().getSymbol, SemanticType.IntLiteral)
}
