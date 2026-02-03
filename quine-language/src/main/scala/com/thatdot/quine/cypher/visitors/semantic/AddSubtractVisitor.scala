package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime.tree.TerminalNode

import com.thatdot.quine.cypher.parsing.CypherParser.OC_MultiplyDivideModuloExpressionContext
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object AddSubtractVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_AddOrSubtractExpression(
    ctx: CypherParser.OC_AddOrSubtractExpressionContext,
  ): List[SemanticToken] =
    ctx.children.asScala.toList.flatMap {
      case childCtx: OC_MultiplyDivideModuloExpressionContext => childCtx.accept(MultiplyDivideModuloVisitor)
      case tnode: TerminalNode =>
        tnode.getSymbol.getText match {
          case " " => List.empty[SemanticToken]
          case "+" => List(SemanticToken.fromToken(tnode.getSymbol, SemanticType.AdditionOperator))
          case _ => List.empty[SemanticToken]
        }
      case _ => List.empty[SemanticToken]
    }
}
