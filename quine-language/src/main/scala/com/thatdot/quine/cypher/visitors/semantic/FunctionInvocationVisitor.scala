package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime.tree.TerminalNode

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object FunctionInvocationVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_FunctionInvocation(ctx: CypherParser.OC_FunctionInvocationContext): List[SemanticToken] = {
    val functionNameOpt = ctx.oC_FunctionName().oC_SymbolicName().getChild(0) match {
      case node: TerminalNode => Some(node.getSymbol)
      case _ => None
    }

    val expressionTokens = ctx.oC_Expression().asScala.toList.flatMap(innerCtx => innerCtx.accept(ExpressionVisitor))

    functionNameOpt match {
      case Some(functionName) =>
        SemanticToken.fromToken(functionName, SemanticType.FunctionApplication) :: expressionTokens
      case None => expressionTokens
    }
  }
}
