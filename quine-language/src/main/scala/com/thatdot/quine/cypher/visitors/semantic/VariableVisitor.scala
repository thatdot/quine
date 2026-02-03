package com.thatdot.quine.cypher.visitors.semantic

import org.antlr.v4.runtime.tree.TerminalNode

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object VariableVisitor extends CypherBaseVisitor[SemanticToken] {
  override def visitOC_Variable(ctx: CypherParser.OC_VariableContext): SemanticToken = {
    val variableToken = ctx.oC_SymbolicName().getChild(0) match {
      case tnode: TerminalNode => tnode.getSymbol
      case other => throw new IllegalStateException(s"Expected TerminalNode but got ${other.getClass.getName}")
    }

    SemanticToken.fromToken(variableToken, SemanticType.Variable)
  }
}
