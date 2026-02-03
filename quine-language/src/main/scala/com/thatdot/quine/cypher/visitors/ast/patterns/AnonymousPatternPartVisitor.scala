package com.thatdot.quine.cypher.visitors.ast.patterns

import com.thatdot.quine.cypher.ast.GraphPattern
import com.thatdot.quine.cypher.parsing.CypherBaseVisitor
import com.thatdot.quine.cypher.parsing.CypherParser.OC_AnonymousPatternPartContext

object AnonymousPatternPartVisitor extends CypherBaseVisitor[Option[GraphPattern]] {
  override def visitOC_AnonymousPatternPart(ctx: OC_AnonymousPatternPartContext): Option[GraphPattern] =
    ctx.oC_PatternElement().accept(PatternElementVisitor)
}
