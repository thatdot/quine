package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.maybeMatch
import com.thatdot.quine.language.semantic.SemanticToken

object WithClauseVisitor extends CypherBaseVisitor[List[SemanticToken]] {

  /** Emits the tokens of a WITH clause: its projection body and optional WHERE. The semantic
    * token legend has no entry for the WITH keyword itself, so it contributes no token.
    */
  override def visitOC_With(ctx: CypherParser.OC_WithContext): List[SemanticToken] = {
    val projectionTokens = ctx.oC_ProjectionBody().accept(ProjectionBodyVisitor)
    val whereTokens = maybeMatch(ctx.oC_Where(), WhereVisitor).getOrElse(List.empty[SemanticToken])

    projectionTokens ::: whereTokens
  }
}
