package com.thatdot.quine.cypher.visitors.ast

import com.thatdot.quine.cypher.ast.Projection
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}

object ReturnVisitor extends CypherBaseVisitor[(Boolean, Boolean, List[Projection])] {
  override def visitOC_Return(ctx: CypherParser.OC_ReturnContext): (Boolean, Boolean, List[Projection]) =
    ctx.oC_ProjectionBody().accept(ProjectionBodyVisitor)
}
