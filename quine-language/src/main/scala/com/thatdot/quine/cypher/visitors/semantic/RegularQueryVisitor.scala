package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_RegularQueryContext
import com.thatdot.quine.language.semantic.SemanticToken

object RegularQueryVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_RegularQuery(ctx: OC_RegularQueryContext): List[SemanticToken] = {
    val init = ctx.oC_SingleQuery().accept(SingleQueryVisitor)
    ctx
      .oC_Union()
      .asScala
      .toList
      .foldLeft(init)((tokens, innerCtx) => tokens ::: innerCtx.oC_SingleQuery().accept(SingleQueryVisitor))
  }
}
