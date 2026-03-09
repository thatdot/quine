package com.thatdot.quine.cypher.visitors.ast

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.ast.{Projection, SortItem}
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.ExpressionVisitor
import com.thatdot.quine.language.ast.{Expression, Source}

/** Parsed result of a projection body (shared between WITH and RETURN). */
case class ProjectionBody(
  hasWildcard: Boolean,
  isDistinct: Boolean,
  projections: List[Projection],
  orderBy: List[SortItem],
  maybeSkip: Option[Expression],
  maybeLimit: Option[Expression],
)

object ProjectionBodyVisitor extends CypherBaseVisitor[ProjectionBody] {
  override def visitOC_ProjectionBody(
    ctx: CypherParser.OC_ProjectionBodyContext,
  ): ProjectionBody =
    Option(ctx.oC_ProjectionItems())
      .map { projectionItems =>
        val projections =
          projectionItems.oC_ProjectionItem().asScala.toList.map(innerCtx => innerCtx.accept(ProjectionItemVisitor))
        val hasWildcard = Option(ctx.oC_ProjectionItems().oC_Wildcard()).isDefined
        val isDistinct = Option(ctx.DISTINCT()).isDefined

        val orderByItems: List[SortItem] = Option(ctx.oC_Order()) match {
          case Some(orderCtx) =>
            orderCtx.oC_SortItem().asScala.toList.map { sortItemCtx =>
              val expr = sortItemCtx.oC_Expression().accept(ExpressionVisitor)
              val ascending = Option(sortItemCtx.DESCENDING()).isEmpty && Option(sortItemCtx.DESC()).isEmpty
              val src = Source.TextSource(
                start = sortItemCtx.start.getStartIndex,
                end = sortItemCtx.stop.getStopIndex,
              )
              SortItem(src, expr, ascending)
            }
          case None => Nil
        }

        val maybeSkip: Option[Expression] = Option(ctx.oC_Skip()).map { skipCtx =>
          skipCtx.oC_Expression().accept(ExpressionVisitor)
        }

        val maybeLimit: Option[Expression] = Option(ctx.oC_Limit()).map { limitCtx =>
          limitCtx.oC_Expression().accept(ExpressionVisitor)
        }

        ProjectionBody(hasWildcard, isDistinct, projections, orderByItems, maybeSkip, maybeLimit)
      }
      .getOrElse(ProjectionBody(false, false, Nil, Nil, None, None))
}
