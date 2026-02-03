package com.thatdot.quine.cypher.visitors.semantic

import cats.implicits._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object SingleQueryVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_SingleQuery(ctx: CypherParser.OC_SingleQueryContext): List[SemanticToken] = {
    val r1 = if (ctx.oC_SinglePartQuery() == null) {
      Option.empty[List[SemanticToken]]
    } else {
      Some(ctx.oC_SinglePartQuery().accept(SinglePartQueryVisitor))
    }
    val r2 = if (ctx.oC_MultiPartQuery() == null) {
      Option.empty[List[SemanticToken]]
    } else {
      Some(ctx.oC_MultiPartQuery().accept(MultiPartQueryVisitor))
    }

    (r1 <+> r2).getOrElse(Nil)
  }
}
