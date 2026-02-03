package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import cats.implicits._
import org.antlr.v4.runtime.tree.TerminalNode

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.{maybeMatch, requireOne}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object LiteralVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_Literal(ctx: CypherParser.OC_LiteralContext): List[SemanticToken] = {
    val maybeNumber = maybeMatch(ctx.oC_NumberLiteral(), NumberVisitor).map(List(_))
    val maybeString = Option(ctx.StringLiteral())
      .map { node =>
        SemanticToken.fromToken(node.getSymbol, SemanticType.StringLiteral)
      }
      .map(List(_))

    // Do we want the actual array brackets to have semantic tags?
    val maybeList = Option(ctx.oC_ListLiteral()).map { ctx =>
      ctx.oC_Expression().asScala.toList.flatMap(childCtx => childCtx.accept(ExpressionVisitor))
    }

    val maybeNull =
      Option(ctx.NULL()).map(node => SemanticToken.fromToken(node.getSymbol, SemanticType.NullLiteral)).map(List(_))

    val maybeBool = Option(ctx.oC_BooleanLiteral())
      .flatMap { boolCtx =>
        boolCtx.getChild(0) match {
          case node: TerminalNode => Some(SemanticToken.fromToken(node.getSymbol, SemanticType.BooleanLiteral))
          case _ => None
        }
      }
      .map(List(_))

    requireOne(maybeNumber <+> maybeString <+> maybeList <+> maybeNull <+> maybeBool, "literal")
  }
}
