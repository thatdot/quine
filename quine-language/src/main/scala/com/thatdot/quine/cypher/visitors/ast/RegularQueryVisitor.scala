package com.thatdot.quine.cypher.visitors.ast

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.ast.Query
import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_RegularQueryContext

object RegularQueryVisitor extends CypherBaseVisitor[Option[Query]] {
  override def visitOC_RegularQuery(ctx: OC_RegularQueryContext): Option[Query] = {
    val unionedQueries = ctx.oC_Union().asScala

    val lhs = ctx.oC_SingleQuery().accept(SingleQueryVisitor)
    if (unionedQueries.isEmpty) {
      lhs
    } else {
      throw new Exception("Need to handle unions better.")
    }
  }
}
