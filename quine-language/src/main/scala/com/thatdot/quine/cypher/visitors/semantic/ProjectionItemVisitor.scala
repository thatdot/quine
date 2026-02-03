package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object ProjectionItemVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_ProjectionItem(ctx: CypherParser.OC_ProjectionItemContext): List[SemanticToken] = {
    val expressionSemanticTokens = ctx.oC_Expression().accept(ExpressionVisitor)

    val aliasTokens = if (null == ctx.AS()) {
      List.empty[SemanticToken]
    } else {
      val asToken = ctx.AS().getSymbol

      val asSemanticToken = SemanticToken.fromToken(asToken, SemanticType.AsKeyword)

      val variableToken = ctx.oC_Variable().oC_SymbolicName().UnescapedSymbolicName().getSymbol

      val variableSemanticToken = SemanticToken.fromToken(variableToken, SemanticType.Variable)

      asSemanticToken :: variableSemanticToken :: Nil
    }

    expressionSemanticTokens ::: aliasTokens
  }
}
