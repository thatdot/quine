package com.thatdot.quine.cypher.visitors.ast.expressions

import scala.jdk.CollectionConverters._

import cats.implicits._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.{maybeMatch, requireOne}
import com.thatdot.quine.language.ast.Expression.CaseBlock
import com.thatdot.quine.language.ast.{Expression, Source, SpecificCase, Value}

object AtomVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_Atom(ctx: CypherParser.OC_AtomContext): Expression = {
    val maybeLiteral = maybeMatch(ctx.oC_Literal(), LiteralVisitor)
    val maybeApply =
      maybeMatch(ctx.oC_FunctionInvocation(), FunctionInvocationVisitor)
    val maybeVariable = maybeMatch(ctx.oC_Variable(), VariableVisitor)
    val maybeParameter =
      maybeMatch(ctx.oC_Parameter(), ParameterVisitor)
    val maybeParenthetical = for {
      mp <- Option(ctx.oC_ParenthesizedExpression())
      me <- Option(mp.oC_Expression())
    } yield me.accept(ExpressionVisitor)

    val maybeCase = for {
      caseCtx <- Option(ctx.oC_CaseExpression())
      alternatives = caseCtx.oC_CaseAlternative().asScala.toList
    } yield {
      val caseBlock = alternatives
        .map(altCtx =>
          SpecificCase(
            altCtx.oC_Expression(0).accept(ExpressionVisitor),
            altCtx.oC_Expression(1).accept(ExpressionVisitor),
          ),
        )
      // ELSE clause is optional in Cypher. When absent, CASE returns null if no condition matches.
      val alternative = Option(caseCtx.oC_Expression(0))
        .map(_.accept(ExpressionVisitor))
        .getOrElse(Expression.AtomicLiteral(Source.NoSource, Value.Null, None))
      CaseBlock(
        Source.TextSource(ctx.start.getStartIndex, ctx.stop.getStopIndex),
        caseBlock,
        alternative,
        None,
      )
    }

    requireOne(
      maybeApply <+> maybeLiteral <+> maybeVariable <+> maybeParameter <+> maybeParenthetical <+> maybeCase,
      s"atom at ${ctx.getText}",
    )
  }
}
