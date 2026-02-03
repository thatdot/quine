package com.thatdot.quine.cypher.visitors.ast.expressions

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{Expression, Source, Value}

object DoubleVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_DoubleLiteral(ctx: CypherParser.OC_DoubleLiteralContext): Expression = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    Expression.AtomicLiteral(src, Value.Real(ctx.RegularDecimalReal().getText.toDouble), None)
  }
}
