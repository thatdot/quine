package com.thatdot.quine.cypher.visitors.ast.patterns

import cats.implicits._

import com.thatdot.quine.cypher.ast.{Connection, GraphPattern}
import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_PatternElementContext
import com.thatdot.quine.cypher.utils.Helpers.{maybeMatch, maybeMatchList}
import com.thatdot.quine.language.ast.Source

object PatternElementVisitor extends CypherBaseVisitor[Option[GraphPattern]] {
  override def visitOC_PatternElement(ctx: OC_PatternElementContext): Option[GraphPattern] = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    val r1: Option[GraphPattern] =
      maybeMatch(ctx.oC_NodePattern(), NodePatternVisitor).map(p => GraphPattern(src, p, Nil))
    val r2 = maybeMatch(ctx.oC_PatternElement(), PatternElementVisitor).flatten

    val r3 = maybeMatchList(ctx.oC_PatternElementChain(), PatternElementChainVisitor)

    val maybeLhs = r1 <+> r2

    val maybeRhs = r3.map(xs => xs.map(c => Connection(c._1, c._2)))

    maybeRhs match {
      case Some(value) =>
        maybeLhs.flatMap { np =>
          if (value.isEmpty)
            maybeLhs
          else
            Some(np.copy(path = value))
        }
      case None => maybeLhs
    }
  }
}
