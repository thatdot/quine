package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime.tree.TerminalNode

import com.thatdot.quine.cypher.parsing.CypherParser.OC_NotExpressionContext
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object AndVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_AndExpression(ctx: CypherParser.OC_AndExpressionContext): List[SemanticToken] =
    ctx.children.asScala.toList.flatMap {
      case childCtx: OC_NotExpressionContext => childCtx.accept(NotVisitor)
      case node: TerminalNode =>
        node.getText match {
          case " " => List.empty[SemanticToken]
          case "" => List.empty[SemanticToken]
          case str if str.trim == "" => List.empty[SemanticToken]
          case "AND" => List(SemanticToken.fromToken(node.getSymbol, SemanticType.AndKeyword))
          case _ => List.empty[SemanticToken]
        }
      case _ => List.empty[SemanticToken]
    }
}
