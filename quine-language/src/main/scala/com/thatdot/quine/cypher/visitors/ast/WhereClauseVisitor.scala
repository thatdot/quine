package com.thatdot.quine.cypher.visitors.ast

import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_WhereContext
import com.thatdot.quine.cypher.visitors.ast.expressions.ExpressionVisitor
import com.thatdot.quine.language.ast.Expression

object WhereClauseVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_Where(ctx: OC_WhereContext): Expression =
    ctx.oC_Expression().accept(ExpressionVisitor)
}
