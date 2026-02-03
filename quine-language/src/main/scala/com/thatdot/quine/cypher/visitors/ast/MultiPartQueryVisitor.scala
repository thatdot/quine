package com.thatdot.quine.cypher.visitors.ast

import scala.jdk.CollectionConverters._

import cats.implicits._

import com.thatdot.quine.cypher.ast.Query.SingleQuery.MultipartQuery
import com.thatdot.quine.cypher.ast.QueryPart
import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_MultiPartQueryContext
import com.thatdot.quine.language.ast.Source

object MultiPartQueryVisitor extends CypherBaseVisitor[Option[MultipartQuery]] {
  override def visitOC_MultiPartQuery(
    ctx: OC_MultiPartQueryContext,
  ): Option[MultipartQuery] =
    for {
      readingClauses <- ctx.oC_ReadingClause().asScala.toList.traverse { innerCtx =>
        innerCtx.accept(ReadingClauseVisitor)
      }
      updatingClauses <- ctx
        .oC_UpdatingClause()
        .asScala
        .toList
        .flatTraverse(innerCtx => innerCtx.accept(UpdatingClauseVisitor))
      singlePartCtx <- Option(ctx.oC_SinglePartQuery())
      into <- singlePartCtx.accept(SinglePartQueryVisitor)
    } yield {
      val withs = ctx
        .oC_With()
        .asScala
        .toList
        .map(innerCtx => innerCtx.accept(WithVisitor))

      val withQps: List[QueryPart] =
        withs.map(wc => QueryPart.WithClausePart(wc))
      val readQps: List[QueryPart] =
        readingClauses.map(rc => QueryPart.ReadingClausePart(rc))
      val effectQps: List[QueryPart] =
        updatingClauses.map(uc => QueryPart.EffectPart(uc))

      val qps = withQps ::: readQps ::: effectQps

      val orderedQps = qps.sortBy {
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

      val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

      MultipartQuery(src, orderedQps, into)
    }
}
