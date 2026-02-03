package com.thatdot.quine.cypher.visitors.ast.patterns

import scala.jdk.CollectionConverters._

import cats.implicits.toSemigroupKOps

import com.thatdot.quine.cypher.ast.NodePattern
import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_NodePatternContext
import com.thatdot.quine.cypher.visitors.ast.NodeLabelVisitor
import com.thatdot.quine.cypher.visitors.ast.expressions.{MapLiteralVisitor, ParameterVisitor}
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Source}

object NodePatternVisitor extends CypherBaseVisitor[NodePattern] {
  override def visitOC_NodePattern(ctx: OC_NodePatternContext): NodePattern = {
    val src = Source.TextSource(
      start = ctx.start.getStartIndex,
      end = ctx.stop.getStopIndex,
    )

    val labels =
      if (ctx.oC_NodeLabels() == null) Set.empty[Symbol]
      else
        ctx
          .oC_NodeLabels()
          .oC_NodeLabel()
          .asScala
          .toList
          .map(innerCtx => innerCtx.accept(NodeLabelVisitor))
          .toSet

    val properties: Option[Expression] = Option(ctx.oC_Properties()).flatMap { prop =>
      val mapLiteralExpr: Option[Expression] =
        Option(prop.oC_MapLiteral()).map(_.accept(MapLiteralVisitor))
      val paramExpr: Option[Expression] =
        Option(prop.oC_Parameter()).map(_.accept(ParameterVisitor))
      mapLiteralExpr <+> paramExpr
    }

    val maybeBinding =
      if (ctx.oC_Variable() == null) Option.empty[Symbol]
      else Some(Symbol(ctx.oC_Variable().getText))

    NodePattern(
      src,
      maybeBinding.map(name => Left(CypherIdentifier(name))),
      labels,
      properties,
    )
  }
}
