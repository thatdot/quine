package com.thatdot.quine.cypher.visitors.ast

import cats.implicits._

import com.thatdot.quine.cypher.ast.ReadingClause
import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_ReadingClauseContext
import com.thatdot.quine.cypher.utils.Helpers.maybeMatch

object ReadingClauseVisitor extends CypherBaseVisitor[Option[ReadingClause]] {
  override def visitOC_ReadingClause(ctx: OC_ReadingClauseContext): Option[ReadingClause] = {
    val r1 = maybeMatch(ctx.oC_Match(), MatchClauseVisitor).flatten

    val r2 = maybeMatch(ctx.oC_Unwind(), UnwindClauseVisitor)

    val r3 = maybeMatch(ctx.oC_InQueryCall(), InQueryCallVisitor).flatten

    (r1 <+> r2 <+> r3)
  }
}
