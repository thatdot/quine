package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object PatternVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_Pattern(ctx: CypherParser.OC_PatternContext): List[SemanticToken] = {
    val patternList = ctx.oC_PatternPart().asScala.toList
    patternList.flatMap(innerCtx => innerCtx.accept(PatternPartVisitor))
  }
}
