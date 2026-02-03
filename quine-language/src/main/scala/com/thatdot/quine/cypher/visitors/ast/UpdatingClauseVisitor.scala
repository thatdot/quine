package com.thatdot.quine.cypher.visitors.ast

import cats.implicits._

import com.thatdot.quine.cypher.ast.Effect
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers

object UpdatingClauseVisitor extends CypherBaseVisitor[Option[List[Effect]]] {
  override def visitOC_UpdatingClause(ctx: CypherParser.OC_UpdatingClauseContext): Option[List[Effect]] = {
    val r1 = Helpers.maybeMatch(ctx.oC_Foreach(), ForeachVisitor).flatten
    val r2 = Helpers.maybeMatch(ctx.oC_Effect(), EffectVisitor).flatten

    (r1 <+> r2)
  }
}
