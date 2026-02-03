package com.thatdot.quine.cypher.visitors.semantic

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object NodeLabelVisitor extends CypherBaseVisitor[SemanticToken] {
  override def visitOC_NodeLabel(ctx: CypherParser.OC_NodeLabelContext): SemanticToken = {
    val labelToken = ctx.oC_LabelName().oC_SchemaName().oC_SymbolicName().UnescapedSymbolicName().getSymbol

    SemanticToken.fromToken(labelToken, SemanticType.NodeLabel)
  }
}
