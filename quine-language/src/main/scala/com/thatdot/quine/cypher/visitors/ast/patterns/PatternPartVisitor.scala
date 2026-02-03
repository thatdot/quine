package com.thatdot.quine.cypher.visitors.ast.patterns

import com.thatdot.quine.cypher.ast.GraphPattern
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}

object PatternPartVisitor extends CypherBaseVisitor[Option[GraphPattern]] {
  override def visitOC_PatternPart(ctx: CypherParser.OC_PatternPartContext): Option[GraphPattern] =
    ctx.oC_AnonymousPatternPart().oC_PatternElement().accept(PatternElementVisitor)
}
