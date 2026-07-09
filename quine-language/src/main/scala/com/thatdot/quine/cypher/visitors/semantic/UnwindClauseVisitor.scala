package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object UnwindClauseVisitor extends CypherBaseVisitor[List[SemanticToken]] {

  /** Emits the tokens of `UNWIND expression AS variable`. The semantic token legend has no
    * entry for the UNWIND keyword itself, so it contributes no token.
    */
  override def visitOC_Unwind(ctx: CypherParser.OC_UnwindContext): List[SemanticToken] = {
    val expressionTokens = ctx.oC_Expression().accept(ExpressionVisitor)
    val asToken = SemanticToken.fromToken(ctx.AS().getSymbol, SemanticType.AsKeyword)
    val variableToken = ctx.oC_Variable().accept(VariableVisitor)

    expressionTokens ::: asToken :: variableToken :: Nil
  }
}
