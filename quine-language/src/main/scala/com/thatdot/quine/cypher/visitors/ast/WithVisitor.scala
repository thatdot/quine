package com.thatdot.quine.cypher.visitors.ast

import com.thatdot.quine.cypher.ast.WithClause
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.Source

object WithVisitor extends CypherBaseVisitor[WithClause] {
  override def visitOC_With(ctx: CypherParser.OC_WithContext): WithClause = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    val maybePred =
      Option.apply(ctx.oC_Where()).map(_.accept(WhereClauseVisitor))

    val body = ctx.oC_ProjectionBody().accept(ProjectionBodyVisitor)

    WithClause(
      src,
      body.hasWildcard,
      body.isDistinct,
      body.projections,
      maybePred,
      body.orderBy,
      body.maybeSkip,
      body.maybeLimit,
    )
  }
}
