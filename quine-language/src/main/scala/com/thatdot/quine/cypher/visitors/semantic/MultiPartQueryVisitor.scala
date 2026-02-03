package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object MultiPartQueryVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_MultiPartQuery(ctx: CypherParser.OC_MultiPartQueryContext): List[SemanticToken] =
    ???
}
