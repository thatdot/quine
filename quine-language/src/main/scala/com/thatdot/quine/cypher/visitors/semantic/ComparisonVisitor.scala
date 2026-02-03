package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object ComparisonVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_ComparisonExpression(ctx: CypherParser.OC_ComparisonExpressionContext): List[SemanticToken] = {
    val init = ctx.oC_StringListNullPredicateExpression().accept(StringListNullVisitor)
    val rest =
      ctx.oC_PartialComparisonExpression.asScala.toList.flatMap(innerCtx => innerCtx.accept(PartialComparisonVisitor))

    init ::: rest
  }
}
