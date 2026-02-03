package com.thatdot.quine.cypher.visitors.semantic

import cats.implicits._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.{maybeMatch, requireOne}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object RelationshipPatternVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_RelationshipPattern(ctx: CypherParser.OC_RelationshipPatternContext): List[SemanticToken] = {
    val edgeNameTokens =
      maybeMatch(ctx.oC_RelationshipDetail().oC_Variable(), VariableVisitor).map(List(_)).getOrElse(Nil)

    val leftArrow = Option(ctx.oC_LeftArrowHead()).map { ctx =>
      SemanticToken(
        line = ctx.start.getLine,
        charOnLine = ctx.start.getCharPositionInLine,
        length = (ctx.stop.getStopIndex + 1) - ctx.start.getStartIndex,
        semanticType = SemanticType.Edge,
        modifiers = 0,
      )
    }

    val rightArrow = Option(ctx.oC_RightArrowHead()).map { ctx =>
      SemanticToken(
        line = ctx.start.getLine,
        charOnLine = ctx.start.getCharPositionInLine,
        length = (ctx.stop.getStopIndex + 1) - ctx.start.getStartIndex,
        semanticType = SemanticType.Edge,
        modifiers = 0,
      )
    }

    edgeNameTokens ::: requireOne((leftArrow <+> rightArrow).map(List(_)), "relationship arrow")
  }
}
