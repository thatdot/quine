package com.thatdot.quine.cypher.visitors.ast

import scala.jdk.CollectionConverters._

import cats.implicits._

import com.thatdot.quine.cypher.ast.Effect
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.patterns.PatternPartVisitor
import com.thatdot.quine.language.ast.Source

object PatternVisitor extends CypherBaseVisitor[Option[Effect]] {
  override def visitOC_Pattern(ctx: CypherParser.OC_PatternContext): Option[Effect] = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)
    val patternList = ctx.oC_PatternPart().asScala.toList

    patternList.traverse(innerCtx => innerCtx.accept(PatternPartVisitor)).map { patterns =>
      Effect.Create(src, patterns)
    }
  }
}
