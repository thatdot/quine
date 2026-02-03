package com.thatdot.quine.cypher.visitors.semantic

import cats.implicits._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.{maybeMatch, requireOne}
import com.thatdot.quine.language.semantic.SemanticToken

object AtomVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_Atom(ctx: CypherParser.OC_AtomContext): List[SemanticToken] = {
    val maybeLiteral = maybeMatch(ctx.oC_Literal(), LiteralVisitor)
    val maybeApply = maybeMatch(ctx.oC_FunctionInvocation(), FunctionInvocationVisitor)
    val maybeVariable = maybeMatch(ctx.oC_Variable(), VariableVisitor).map(List(_))
    val maybeParameter = maybeMatch(ctx.oC_Parameter(), ParameterVisitor).map(List(_))
    val maybeParenthetical = for {
      mp <- Option(ctx.oC_ParenthesizedExpression())
      me <- Option(mp.oC_Expression())
    } yield me.accept(ExpressionVisitor)

    requireOne(maybeApply <+> maybeLiteral <+> maybeVariable <+> maybeParameter <+> maybeParenthetical, "atom")
  }
}
