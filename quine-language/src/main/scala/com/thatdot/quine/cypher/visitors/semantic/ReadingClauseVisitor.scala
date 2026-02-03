package com.thatdot.quine.cypher.visitors.semantic

import cats.implicits._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.{maybeMatch, requireOne}
import com.thatdot.quine.language.semantic.SemanticToken

object ReadingClauseVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_ReadingClause(ctx: CypherParser.OC_ReadingClauseContext): List[SemanticToken] = {
    val r1 = maybeMatch(ctx.oC_Match(), MatchClauseVisitor)

    val r2 = maybeMatch(ctx.oC_Unwind(), UnwindClauseVisitor)

    val r3 = maybeMatch(ctx.oC_InQueryCall(), InQueryCallVisitor)

    requireOne(r1 <+> r2 <+> r3, "reading clause")
  }
}
