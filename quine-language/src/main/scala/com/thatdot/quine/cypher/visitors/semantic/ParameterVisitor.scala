package com.thatdot.quine.cypher.visitors.semantic

import org.antlr.v4.runtime.tree.TerminalNode

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object ParameterVisitor extends CypherBaseVisitor[SemanticToken] {
  override def visitOC_Parameter(ctx: CypherParser.OC_ParameterContext): SemanticToken = {
    val nameNode = ctx.oC_SymbolicName().getChild(0) match {
      case node: TerminalNode => node.getSymbol
      case other => throw new IllegalStateException(s"Expected TerminalNode but got ${other.getClass.getName}")
    }

    SemanticToken.fromToken(nameNode, SemanticType.Parameter)
  }
}
