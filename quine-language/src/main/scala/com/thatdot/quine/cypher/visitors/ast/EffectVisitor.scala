package com.thatdot.quine.cypher.visitors.ast

import scala.jdk.CollectionConverters._

import cats.implicits._

import com.thatdot.quine.cypher.ast.Effect
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers

object EffectVisitor extends CypherBaseVisitor[Option[List[Effect]]] {
  override def visitOC_Effect(ctx: CypherParser.OC_EffectContext): Option[List[Effect]] = {
    val maybeSets = Option(ctx.oC_Set()).flatMap(_.oC_SetItem().asScala.toList.traverse(_.accept(SetItemVisitor)))

    val maybeCreate = Helpers.maybeMatch(ctx.oC_Create(), CreateVisitor).map(e => List(e)).flatMap(_.sequence)

    (maybeSets <+> maybeCreate)
  }

}
