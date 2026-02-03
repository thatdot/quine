package com.thatdot.quine.cypher.visitors.ast.expressions

import scala.jdk.CollectionConverters._

import cats.implicits._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.{maybeMatch, requireOne}
import com.thatdot.quine.language.ast.{Expression, Source, Value}

object LiteralVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_Literal(ctx: CypherParser.OC_LiteralContext): Expression = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    val maybeNumber = maybeMatch(ctx.oC_NumberLiteral(), NumberVisitor)
    val maybeString = if (ctx.StringLiteral() == null) {
      Option.empty[Expression.AtomicLiteral]
    } else {
      val rawText = ctx.StringLiteral().getText
      val trimmed = rawText.substring(1, rawText.length - 1)
      val unescaped = StringContext.processEscapes(trimmed)
      Some(Expression.AtomicLiteral(src, Value.Text(unescaped), None))
    }

    val maybeList = if (ctx.oC_ListLiteral() == null) {
      Option.empty[Expression.ListLiteral]
    } else {
      val subExpsList = ctx.oC_ListLiteral().oC_Expression().asScala.toList
      val subExps = subExpsList.map(ectx => ectx.accept(ExpressionVisitor))
      Some(Expression.ListLiteral(src, subExps, None))
    }

    val maybeNull = Option(ctx.NULL()).map(_ => Expression.AtomicLiteral(src, Value.Null, None))

    val maybeBool = Option(ctx.oC_BooleanLiteral()).flatMap { boolCtx =>
      val maybeTrue: Option[Value] = Option(boolCtx.TRUE()).map(_ => Value.True)
      val maybeFalse: Option[Value] = Option(boolCtx.FALSE()).map(_ => Value.False)

      (maybeTrue <+> maybeFalse).map(v => Expression.AtomicLiteral(src, v, None))
    }

    val maybeMapLiteral = maybeMatch(ctx.oC_MapLiteral(), MapLiteralVisitor)

    requireOne(
      maybeNumber <+> maybeString <+> maybeList <+> maybeNull <+> maybeBool <+> maybeMapLiteral,
      s"literal at ${ctx.getText}",
    )
  }
}
