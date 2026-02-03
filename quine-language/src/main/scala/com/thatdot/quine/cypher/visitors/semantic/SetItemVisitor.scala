package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import cats.implicits._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.{maybeMatch, requireOne}
import com.thatdot.quine.language.semantic.SemanticToken

object SetItemVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_SetItem(ctx: CypherParser.OC_SetItemContext): List[SemanticToken] = {
    val maybeSetLabel = for {
      mv <- Option(ctx.oC_Variable())
      ml <- Option(ctx.oC_NodeLabels())
    } yield {
      val labels = ml.oC_NodeLabel().asScala.toList.map(ctx => ctx.accept(NodeLabelVisitor))
      val binding = mv.accept(VariableVisitor)
      binding :: labels
    }

    val maybeSetProperty = for {
      lhs <- maybeMatch(ctx.oC_PropertyExpression(), PropertyExpressionVisitor)
      rhs <- maybeMatch(ctx.oC_Expression(), ExpressionVisitor)
    } yield lhs ::: rhs

    val maybeSetProperties = for {
      varName <- Option(ctx.oC_Variable())
      rhs <- maybeMatch(ctx.oC_Expression(), ExpressionVisitor)
    } yield {
      val varToken = varName.accept(VariableVisitor)
      varToken :: rhs
    }

    requireOne(maybeSetLabel <+> maybeSetProperty <+> maybeSetProperties, "set item")
  }
}
