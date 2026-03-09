package com.thatdot.quine.cypher.visitors.ast

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}

object ReturnVisitor extends CypherBaseVisitor[ProjectionBody] {
  override def visitOC_Return(ctx: CypherParser.OC_ReturnContext): ProjectionBody =
    ctx.oC_ProjectionBody().accept(ProjectionBodyVisitor)
}
