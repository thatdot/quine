package com.thatdot.quine.cypher.visitors.ast.expressions

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.Expression.MapLiteral
import com.thatdot.quine.language.ast.Source

object MapLiteralVisitor extends CypherBaseVisitor[MapLiteral] {
  override def visitOC_MapLiteral(ctx: CypherParser.OC_MapLiteralContext): MapLiteral = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    val propNames = ctx.oC_PropertyKeyName().asScala.toList.map(innerCtx => Symbol(innerCtx.getText))
    val propValues = ctx.oC_Expression().asScala.toList.map(innerCtx => innerCtx.accept(ExpressionVisitor))

    MapLiteral(
      source = src,
      value = propNames.zip(propValues).toMap,
      ty = None,
    )
  }
}
