package com.thatdot.quine.cypher.visitors.ast

import scala.jdk.CollectionConverters._

import cats.implicits._

import com.thatdot.quine.cypher.ast.Effect
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.ExpressionVisitor
import com.thatdot.quine.language.ast.Source

object ForeachVisitor extends CypherBaseVisitor[Option[List[Effect]]] {
  override def visitOC_Foreach(
    ctx: CypherParser.OC_ForeachContext,
  ): Option[List[Effect]] = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    val id = ctx.oC_Variable().getText
    val exp = ctx.oC_Expression().accept(ExpressionVisitor)
    val maybeEffects =
      ctx.oC_Effect.asScala.toList.flatTraverse(_.accept(EffectVisitor))

    maybeEffects.map(effects => List(Effect.Foreach(src, Symbol(id), exp, effects)))
  }
}
