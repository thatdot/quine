package com.thatdot.quine.cypher.visitors.ast

import com.thatdot.quine.cypher.ast.ReadingClause
import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_MatchContext
import com.thatdot.quine.cypher.visitors.ast.patterns.MatchPatternVisitor
import com.thatdot.quine.language.ast.Source

object MatchClauseVisitor extends CypherBaseVisitor[Option[ReadingClause]] {
  override def visitOC_Match(ctx: OC_MatchContext): Option[ReadingClause] = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    val maybeWhere = Option(ctx.oC_Where()).map(_.accept(WhereClauseVisitor))

    for {
      patterns <- ctx.oC_Pattern().accept(MatchPatternVisitor)
    } yield ReadingClause.FromPatterns(
      source = src,
      patterns = patterns,
      maybePredicate = maybeWhere,
    )
  }
}
