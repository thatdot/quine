package com.thatdot.quine.cypher.visitors.ast

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}

object NodeLabelVisitor extends CypherBaseVisitor[Symbol] {
  override def visitOC_NodeLabel(ctx: CypherParser.OC_NodeLabelContext): Symbol =
    Symbol(ctx.getText.substring(1).trim)
}
