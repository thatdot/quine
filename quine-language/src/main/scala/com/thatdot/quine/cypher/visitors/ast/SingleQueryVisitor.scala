package com.thatdot.quine.cypher.visitors.ast

import cats.implicits._

import com.thatdot.quine.cypher.ast.Query.SingleQuery
import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_SingleQueryContext
import com.thatdot.quine.cypher.utils.Helpers.maybeMatch

object SingleQueryVisitor extends CypherBaseVisitor[Option[SingleQuery]] {
  override def visitOC_SingleQuery(ctx: OC_SingleQueryContext): Option[SingleQuery] = {
    val r1: Option[SingleQuery] = maybeMatch(ctx.oC_SinglePartQuery(), SinglePartQueryVisitor).flatten

    val r2 = maybeMatch(ctx.oC_MultiPartQuery(), MultiPartQueryVisitor).flatten

    (r1 <+> r2)
  }
}
