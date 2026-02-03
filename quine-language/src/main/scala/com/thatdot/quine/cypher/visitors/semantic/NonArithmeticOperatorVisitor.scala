package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object NonArithmeticOperatorVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_NonArithmeticOperatorExpression(
    ctx: CypherParser.OC_NonArithmeticOperatorExpressionContext,
  ): List[SemanticToken] =
    ctx.oC_Atom().accept(AtomVisitor)
}
