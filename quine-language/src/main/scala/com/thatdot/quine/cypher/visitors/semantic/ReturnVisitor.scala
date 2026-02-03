package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object ReturnVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_Return(ctx: CypherParser.OC_ReturnContext): List[SemanticToken] = {
    val returnToken = ctx.RETURN().getSymbol
    val returnSemanticToken = SemanticToken.fromToken(returnToken, SemanticType.ReturnKeyword)
    returnSemanticToken :: ctx.oC_ProjectionBody().accept(ProjectionBodyVisitor)
  }
}
