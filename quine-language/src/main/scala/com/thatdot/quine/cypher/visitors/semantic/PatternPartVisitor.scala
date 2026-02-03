package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime.tree.TerminalNode

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object PatternPartVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_PatternPart(ctx: CypherParser.OC_PatternPartContext): List[SemanticToken] = {
    val lhs = if (null == ctx.oC_Variable()) {
      List.empty[SemanticToken]
    } else {
      val variableToken = ctx.oC_Variable().oC_SymbolicName().UnescapedSymbolicName().getSymbol
      val equalsToken = ctx.children.asScala.toList
        .collectFirst {
          case node: TerminalNode if node.getText == "=" => node.getSymbol
        }
        .getOrElse(throw new IllegalStateException(s"Expected '=' in pattern part at ${ctx.getText}"))

      val variableSemanticToken = SemanticToken.fromToken(variableToken, SemanticType.PatternVariable)

      val equalsSemanticToken = SemanticToken.fromToken(equalsToken, SemanticType.AssignmentOperator)

      variableSemanticToken :: equalsSemanticToken :: Nil
    }

    val rhs = ctx.oC_AnonymousPatternPart().accept(AnonymousPatternPartVisitor)

    lhs ::: rhs
  }
}
