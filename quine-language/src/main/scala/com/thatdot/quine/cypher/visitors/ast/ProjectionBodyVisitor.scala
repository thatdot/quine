package com.thatdot.quine.cypher.visitors.ast

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.ast.Projection
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}

object ProjectionBodyVisitor extends CypherBaseVisitor[(Boolean, Boolean, List[Projection])] {
  override def visitOC_ProjectionBody(
    ctx: CypherParser.OC_ProjectionBodyContext,
  ): (Boolean, Boolean, List[Projection]) =
    Option(ctx.oC_ProjectionItems())
      .map { projectionItems =>
        val projections =
          projectionItems.oC_ProjectionItem().asScala.toList.map(innerCtx => innerCtx.accept(ProjectionItemVisitor))
        val hasWildcard = Option(ctx.oC_ProjectionItems().oC_Wildcard()).isDefined
        val isDistinct = Option(ctx.DISTINCT()).isDefined
        (hasWildcard, isDistinct, projections)
      }
      .getOrElse((false, false, Nil))
}
