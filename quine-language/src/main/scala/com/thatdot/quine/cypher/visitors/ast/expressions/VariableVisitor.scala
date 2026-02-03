package com.thatdot.quine.cypher.visitors.ast.expressions

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Source}

object VariableVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_Variable(ctx: CypherParser.OC_VariableContext): Expression = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)
    Expression.Ident(src, Left(CypherIdentifier(Symbol(ctx.oC_SymbolicName().getText))), None)
  }
}
