package com.thatdot.quine.cypher.visitors.ast

import scala.jdk.CollectionConverters._

import cats.syntax.traverse._

import com.thatdot.quine.cypher.ast.Query
import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_RegularQueryContext
import com.thatdot.quine.language.ast.Source

object RegularQueryVisitor extends CypherBaseVisitor[Option[Query]] {
  override def visitOC_RegularQuery(ctx: OC_RegularQueryContext): Option[Query] = {
    val unions = ctx.oC_Union().asScala.toList
    val first: Option[Query.SingleQuery] = ctx.oC_SingleQuery().accept(SingleQueryVisitor)
    first.flatMap { firstQuery =>
      unions
        .traverse { u =>
          u.oC_SingleQuery().accept(SingleQueryVisitor).map { sq =>
            val isAll = u.ALL() != null
            val src = Source.TextSource(start = u.start.getStartIndex, end = u.stop.getStopIndex)
            (isAll, sq, src)
          }
        }
        .map { parsed =>
          // Build left-associative tree: A UNION B UNION C => Union(Union(A, B), C)
          parsed.foldLeft(firstQuery: Query) { case (lhs, (isAll, rhs, source)) =>
            Query.Union(source, isAll, lhs, rhs)
          }
        }
    }
  }
}
