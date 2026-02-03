package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.CypherParser.OC_SchemaNameContext
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object PropertyExpressionVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_PropertyExpression(ctx: CypherParser.OC_PropertyExpressionContext): List[SemanticToken] = {
    val of = ctx.oC_Atom().accept(AtomVisitor)
    val props = ctx.oC_PropertyLookup.asScala.toList.flatMap { ctx =>
      ctx.oC_PropertyKeyName().getChild(0) match {
        case schema: OC_SchemaNameContext =>
          Some(
            SemanticToken(
              line = schema.getStart.getLine,
              charOnLine = schema.getStart.getCharPositionInLine,
              length = (schema.getStop.getStopIndex + 1) - schema.getStart.getStartIndex,
              semanticType = SemanticType.Property,
              modifiers = 0,
            ),
          )
        case _ => None
      }
    }

    of ::: props
  }
}
