package com.thatdot.quine.cypher.visitors.ast.expressions

import cats.implicits._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.{maybeMatch, requireOne}
import com.thatdot.quine.language.ast.Expression

object NumberVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_NumberLiteral(ctx: CypherParser.OC_NumberLiteralContext): Expression = {
    val maybeDouble = maybeMatch(ctx.oC_DoubleLiteral(), DoubleVisitor)
    val maybeInt = maybeMatch(ctx.oC_IntegerLiteral(), IntegerVisitor)

    requireOne(maybeDouble <+> maybeInt, s"number literal at ${ctx.getText}")
  }
}
