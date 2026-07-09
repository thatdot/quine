package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object MapLiteralVisitor extends CypherBaseVisitor[List[SemanticToken]] {

  /** Emits a Property token for each map key followed by the tokens of its value expression,
    * in document order. The braces, colons, and commas have no semantic token legend entry.
    * The grammar pairs every key with exactly one value expression, so the two context lists
    * zip without loss.
    */
  override def visitOC_MapLiteral(ctx: CypherParser.OC_MapLiteralContext): List[SemanticToken] = {
    val keys = ctx.oC_PropertyKeyName().asScala.toList
    val values = ctx.oC_Expression().asScala.toList

    keys.zip(values).flatMap { case (key, value) =>
      SemanticToken.fromContext(key, SemanticType.Property) :: value.accept(ExpressionVisitor)
    }
  }
}
