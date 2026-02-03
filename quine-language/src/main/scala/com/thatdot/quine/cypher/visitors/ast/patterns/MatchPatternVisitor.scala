package com.thatdot.quine.cypher.visitors.ast.patterns

import scala.jdk.CollectionConverters._

import cats.implicits._

import com.thatdot.quine.cypher.ast.GraphPattern
import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_PatternContext

object MatchPatternVisitor extends CypherBaseVisitor[Option[List[GraphPattern]]] {
  override def visitOC_Pattern(ctx: OC_PatternContext): Option[List[GraphPattern]] = {
    val parts = ctx.oC_PatternPart().asScala.toList

    parts.traverse(innerCtx => innerCtx.accept(PatternExpVisitor))
  }
}
