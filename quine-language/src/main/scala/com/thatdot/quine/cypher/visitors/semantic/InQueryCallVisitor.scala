package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object InQueryCallVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_InQueryCall(ctx: CypherParser.OC_InQueryCallContext): List[SemanticToken] =
    ???
}
