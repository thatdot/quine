package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime.ParserRuleContext

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.maybeMatch
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object RelationshipPatternVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_RelationshipPattern(ctx: CypherParser.OC_RelationshipPatternContext): List[SemanticToken] = {
    // An anonymous (`-->`), empty-bracket (`-[]->`), or untyped (`-[r]->`) relationship has no
    // relationship detail, so `oC_RelationshipDetail()` is null. Guard it with Option before
    // dereferencing `oC_Variable()` (matching the AST visitor).
    val edgeNameTokens =
      Option(ctx.oC_RelationshipDetail())
        .flatMap(detail => maybeMatch(detail.oC_Variable(), VariableVisitor))
        .map(List(_))
        .getOrElse(Nil)

    def edgeToken(node: ParserRuleContext): SemanticToken =
      SemanticToken(
        line = node.start.getLine,
        charOnLine = node.start.getCharPositionInLine,
        length = (node.stop.getStopIndex + 1) - node.start.getStartIndex,
        semanticType = SemanticType.Edge,
        modifiers = 0,
      )

    // A directed relationship highlights its arrow head (left-preferred). An undirected one (`--`,
    // `-[r]-`) has no arrow head — Quine does not support it (the analysis pipeline reports that
    // separately), but still highlight the two dashes so the buffer keeps its highlighting.
    val directedArrow: Option[ParserRuleContext] =
      Option(ctx.oC_LeftArrowHead()).orElse(Option(ctx.oC_RightArrowHead()))
    val edgeTokens: List[SemanticToken] =
      directedArrow match {
        case Some(arrow) => List(edgeToken(arrow))
        case None => ctx.oC_Dash().asScala.toList.map(edgeToken)
      }

    edgeNameTokens ::: edgeTokens
  }
}
