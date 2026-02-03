package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object MapLiteralVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_MapLiteral(ctx: CypherParser.OC_MapLiteralContext): List[SemanticToken] =
    ???
}
