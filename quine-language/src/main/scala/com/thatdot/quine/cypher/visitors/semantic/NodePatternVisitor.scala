package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object NodePatternVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_NodePattern(ctx: CypherParser.OC_NodePatternContext): List[SemanticToken] = {
    val labelSemanticTokens =
      if (ctx.oC_NodeLabels() == null) List.empty[SemanticToken]
      else ctx.oC_NodeLabels().oC_NodeLabel().asScala.toList.map(innerCtx => innerCtx.accept(NodeLabelVisitor))

    val properties = Option(ctx.oC_Properties()).map(_.oC_MapLiteral().accept(MapLiteralVisitor))

    val rest = labelSemanticTokens ::: properties.getOrElse(List.empty[SemanticToken])

    if (null == ctx.oC_Variable()) {
      rest
    } else {
      val bindingToken = ctx.oC_Variable().accept(VariableVisitor)

      bindingToken :: rest
    }
  }
}
