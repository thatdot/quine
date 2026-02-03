package com.thatdot.quine.cypher.visitors.ast

import scala.jdk.CollectionConverters._

import cats.implicits._

import com.thatdot.quine.cypher.ast.Effect
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.maybeMatch
import com.thatdot.quine.cypher.visitors.ast.expressions.{ExpressionVisitor, PropertyVisitor}
import com.thatdot.quine.language.ast.{CypherIdentifier, Source}

object SetItemVisitor extends CypherBaseVisitor[Option[Effect]] {
  override def visitOC_SetItem(ctx: CypherParser.OC_SetItemContext): Option[Effect] = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    val maybeSetLabel: Option[Effect] = for {
      mv <- Option(ctx.oC_Variable())
      ml <- Option(ctx.oC_NodeLabels())
    } yield {
      val labels = ml.oC_NodeLabel().asScala.map(ctx => ctx.accept(NodeLabelVisitor)).toSet
      val on = Left(CypherIdentifier(Symbol(mv.getText)))
      Effect.SetLabel(src, on, labels)
    }

    val maybeSetProperty: Option[Effect] = for {
      lhs <- maybeMatch(ctx.oC_PropertyExpression(), PropertyVisitor)
      rhs <- maybeMatch(ctx.oC_Expression(), ExpressionVisitor)
    } yield Effect.SetProperty(src, lhs, rhs)

    val maybeSetProperties: Option[Effect] = for {
      varName <- Option(ctx.oC_Variable())
      rhs <- maybeMatch(ctx.oC_Expression(), ExpressionVisitor)
    } yield Effect.SetProperties(src, Left(CypherIdentifier(Symbol(varName.getText))), rhs)

    (maybeSetLabel <+> maybeSetProperty <+> maybeSetProperties)
  }
}
