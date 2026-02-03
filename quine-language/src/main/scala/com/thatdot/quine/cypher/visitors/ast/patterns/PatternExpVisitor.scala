package com.thatdot.quine.cypher.visitors.ast.patterns

import com.thatdot.quine.cypher.ast.GraphPattern
import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_PatternPartContext
import com.thatdot.quine.cypher.utils.Helpers.maybeMatch

object PatternExpVisitor extends CypherBaseVisitor[Option[GraphPattern]] {

  override def visitOC_PatternPart(ctx: OC_PatternPartContext): Option[GraphPattern] = {

    val r1 = maybeMatch(ctx.oC_AnonymousPatternPart(), AnonymousPatternPartVisitor).flatten

    //TODO Is this the quantified pattern visitor?
    //val r2 = maybeMatch(ctx.oC_Variable(), VariablePatternVisitor)

    r1
  }
}
