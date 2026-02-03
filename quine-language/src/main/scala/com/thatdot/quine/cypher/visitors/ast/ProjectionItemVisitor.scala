package com.thatdot.quine.cypher.visitors.ast

import com.thatdot.quine.cypher.ast.Projection
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.ExpressionVisitor
import com.thatdot.quine.language.ast.{CypherIdentifier, Source}

object ProjectionItemVisitor extends CypherBaseVisitor[Projection] {
  override def visitOC_ProjectionItem(ctx: CypherParser.OC_ProjectionItemContext): Projection = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    val expression = ctx.oC_Expression().accept(ExpressionVisitor)
    val alias = if (ctx.oC_Variable() == null) {
      ctx.oC_Expression().getText
    } else {
      ctx.oC_Variable().getText
    }

    Projection(src, expression, Left(CypherIdentifier(Symbol(alias))))
  }

}
