package com.thatdot.quine.cypher.visitors.ast.patterns

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.UnsupportedCypherFeature
import com.thatdot.quine.cypher.ast.EdgePattern
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{CypherIdentifier, Direction, Source}

object RelationshipPatternVisitor extends CypherBaseVisitor[EdgePattern] {
  override def visitOC_RelationshipPattern(ctx: CypherParser.OC_RelationshipPatternContext): EdgePattern = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    val maybeBinding = for {
      rd <- Option(ctx.oC_RelationshipDetail())
      name <- Option(rd.oC_Variable())
    } yield Symbol(name.getText)

    // An anonymous (`-->`), empty-bracket (`-[]->`), or named-but-untyped (`-[r]->`) relationship has
    // no relationship types, so `oC_RelationshipTypes()` is null. Wrap each nullable accessor in Option
    // so the null short-circuits the comprehension to an empty set instead of throwing an NPE.
    val edgeTypes = (for {
      rd <- Option(ctx.oC_RelationshipDetail())
      relTypes <- Option(rd.oC_RelationshipTypes())
      relTypeNames <- Option(relTypes.oC_RelTypeName())
    } yield relTypeNames.asScala.toList.map(labelCtx => Symbol(labelCtx.getText)).toSet)
      .getOrElse(Set.empty[Symbol])

    // Quine relationships are directed. An undirected relationship (`--`, `-[r]-`) has neither arrow
    // head; signal it as an unsupported feature, which ParserPhase turns into a clean diagnostic.
    val direction = if (ctx.oC_LeftArrowHead() != null) {
      Direction.Left
    } else if (ctx.oC_RightArrowHead() != null) {
      Direction.Right
    } else {
      throw UnsupportedCypherFeature(
        "Undirected relationships are not supported; Quine relationships must be directed (use --> or <--)",
      )
    }

    EdgePattern(
      source = src,
      maybeBinding = maybeBinding.map(name => Left(CypherIdentifier(name))),
      edgeTypes = edgeTypes,
      direction = direction,
    )
  }
}
