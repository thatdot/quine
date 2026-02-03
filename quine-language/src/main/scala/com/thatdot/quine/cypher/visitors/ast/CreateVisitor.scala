package com.thatdot.quine.cypher.visitors.ast

import com.thatdot.quine.cypher.ast.Effect
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}

object CreateVisitor extends CypherBaseVisitor[Option[Effect]] {
  override def visitOC_Create(ctx: CypherParser.OC_CreateContext): Option[Effect] =
    ctx.oC_Pattern().accept(PatternVisitor)
}
