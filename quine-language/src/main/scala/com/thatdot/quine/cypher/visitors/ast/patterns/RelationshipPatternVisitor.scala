package com.thatdot.quine.cypher.visitors.ast.patterns

import scala.jdk.CollectionConverters._

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

    val edgeTypes = (for {
      rd <- Option(ctx.oC_RelationshipDetail())
      labels <- Option(rd.oC_RelationshipTypes().oC_RelTypeName().asScala.toList)
    } yield labels.map(labelCtx => Symbol(labelCtx.getText)).toSet).getOrElse(Set.empty[Symbol])

    val direction = if (ctx.oC_LeftArrowHead() != null) {
      Direction.Left
    } else if (ctx.oC_RightArrowHead() != null) {
      Direction.Right
    } else {
      throw new RuntimeException("Yikes!")
    }

    EdgePattern(
      source = src,
      maybeBinding = maybeBinding.map(name => Left(CypherIdentifier(name))),
      edgeType = edgeTypes.head,
      direction = direction,
    )
  }
}
