package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object UnwindClauseVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_Unwind(ctx: CypherParser.OC_UnwindContext): List[SemanticToken] =
    ???
}
