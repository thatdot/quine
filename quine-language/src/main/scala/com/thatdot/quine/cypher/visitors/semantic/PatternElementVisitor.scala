package com.thatdot.quine.cypher.visitors.semantic

import cats.implicits._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.{maybeMatch, maybeMatchList, requireOne}
import com.thatdot.quine.language.semantic.SemanticToken

object PatternElementVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_PatternElement(ctx: CypherParser.OC_PatternElementContext): List[SemanticToken] = {
    val r1: Option[List[SemanticToken]] = maybeMatch(ctx.oC_NodePattern(), NodePatternVisitor)
    val r2: Option[List[SemanticToken]] = maybeMatch(ctx.oC_PatternElement(), PatternElementVisitor)

    val r3: Option[List[List[SemanticToken]]] = maybeMatchList(ctx.oC_PatternElementChain(), PatternElementChainVisitor)

    val lhs = requireOne(r1 <+> r2, "pattern element")

    lhs ::: r3.getOrElse(List.empty[List[SemanticToken]]).flatten
  }
}
