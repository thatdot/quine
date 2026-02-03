package com.thatdot.quine.cypher.visitors.ast

import scala.jdk.CollectionConverters._

import cats.implicits._

import com.thatdot.quine.cypher.ast.Query.SingleQuery.SinglepartQuery
import com.thatdot.quine.cypher.ast.QueryPart
import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_SinglePartQueryContext
import com.thatdot.quine.cypher.utils.Helpers.maybeMatch
import com.thatdot.quine.language.ast.Source

object SinglePartQueryVisitor extends CypherBaseVisitor[Option[SinglepartQuery]] {
  override def visitOC_SinglePartQuery(
    ctx: OC_SinglePartQueryContext,
  ): Option[SinglepartQuery] = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    val readingClauseCtxs = ctx.oC_ReadingClause().asScala.toList

    for {
      readingClauses <- readingClauseCtxs.traverse(innerCtx => innerCtx.accept(ReadingClauseVisitor))
      effects <- ctx
        .oC_UpdatingClause()
        .asScala
        .toList
        .flatTraverse(_.accept(UpdatingClauseVisitor))
    } yield {
      val rcQps = readingClauses.map(rc => QueryPart.ReadingClausePart(rc))
      val efQps = effects.map(e => QueryPart.EffectPart(e))

      val queryParts = rcQps ::: efQps

      val orderedQueryParts = queryParts.sortBy {
        case QueryPart.ReadingClausePart(readingClause) =>
          readingClause.source match {
            case Source.TextSource(start, _) => start
            case Source.NoSource => -1
          }
        case QueryPart.WithClausePart(withClause) =>
          withClause.source match {
            case Source.TextSource(start, _) => start
            case Source.NoSource => -1
          }
        case QueryPart.EffectPart(effect) =>
          effect.source match {
            case Source.TextSource(start, _) => start
            case Source.NoSource => -1
          }
      }

      val (hasWildcard, isDistinct, projs) =
        maybeMatch(ctx.oC_Return(), ReturnVisitor)
          .getOrElse((false, false, Nil))

      SinglepartQuery(
        source = src,
        queryParts = orderedQueryParts,
        hasWildcard = hasWildcard,
        isDistinct = isDistinct,
        bindings = projs,
      )
    }
  }
}
