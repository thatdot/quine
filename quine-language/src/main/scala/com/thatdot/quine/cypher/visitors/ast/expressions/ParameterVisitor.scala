package com.thatdot.quine.cypher.visitors.ast.expressions

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{Expression, Source}

object ParameterVisitor extends CypherBaseVisitor[Expression.Parameter] {
  override def visitOC_Parameter(ctx: CypherParser.OC_ParameterContext): Expression.Parameter = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    Expression.Parameter(src, Symbol(ctx.getText), None)
  }
}
