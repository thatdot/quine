package com.thatdot.quine.cypher.visitors.ast

import com.thatdot.quine.cypher.ast.ReadingClause
import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_UnwindContext
import com.thatdot.quine.cypher.visitors.ast.expressions.ExpressionVisitor
import com.thatdot.quine.language.ast.{CypherIdentifier, Source}

object UnwindClauseVisitor extends CypherBaseVisitor[ReadingClause] {
  override def visitOC_Unwind(ctx: OC_UnwindContext): ReadingClause = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    ReadingClause.FromUnwind(
      src,
      ctx.oC_Expression().accept(ExpressionVisitor),
      Left(CypherIdentifier(Symbol(ctx.oC_Variable().getText))),
    )
  }
}
