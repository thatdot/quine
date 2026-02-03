package com.thatdot.quine.cypher.visitors.ast.expressions

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{Expression, Source, Value}

object IntegerVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_IntegerLiteral(ctx: CypherParser.OC_IntegerLiteralContext): Expression = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    val value: Long = if (ctx.DecimalInteger() != null) {
      ctx.DecimalInteger().getText.toLong
    } else if (ctx.HexInteger() != null) {
      // HexInteger format: 0x followed by hex digits (e.g., 0x1A, 0xFF)
      val hexText = ctx.HexInteger().getText.substring(2) // Remove "0x" prefix
      java.lang.Long.parseLong(hexText, 16)
    } else if (ctx.OctalInteger() != null) {
      // OctalInteger format: 0o followed by octal digits (e.g., 0o17, 0o777)
      val octalText = ctx.OctalInteger().getText.substring(2) // Remove "0o" prefix
      java.lang.Long.parseLong(octalText, 8)
    } else {
      throw new IllegalStateException(s"Unknown integer format: ${ctx.getText}")
    }

    Expression.AtomicLiteral(src, Value.Integer(value), None)
  }
}
