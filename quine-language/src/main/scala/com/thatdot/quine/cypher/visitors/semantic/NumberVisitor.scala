package com.thatdot.quine.cypher.visitors.semantic

import cats.implicits._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.{maybeMatch, requireOne}
import com.thatdot.quine.language.semantic.SemanticToken

object NumberVisitor extends CypherBaseVisitor[SemanticToken] {
  override def visitOC_NumberLiteral(ctx: CypherParser.OC_NumberLiteralContext): SemanticToken = {
    val maybeDouble = maybeMatch(ctx.oC_DoubleLiteral(), DoubleVisitor)
    val maybeInt = maybeMatch(ctx.oC_IntegerLiteral(), IntegerVisitor)

    requireOne(maybeDouble <+> maybeInt, "number literal")
  }
}
