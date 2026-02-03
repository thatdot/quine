package com.thatdot.quine.cypher.visitors.ast.patterns

import com.thatdot.quine.cypher.ast.{EdgePattern, NodePattern}
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}

object PatternElementChainVisitor extends CypherBaseVisitor[(EdgePattern, NodePattern)] {
  override def visitOC_PatternElementChain(
    ctx: CypherParser.OC_PatternElementChainContext,
  ): (EdgePattern, NodePattern) = {

    val dest = ctx.oC_NodePattern().accept(NodePatternVisitor)

    val rel = ctx.oC_RelationshipPattern().accept(RelationshipPatternVisitor)

    rel -> dest
  }
}
