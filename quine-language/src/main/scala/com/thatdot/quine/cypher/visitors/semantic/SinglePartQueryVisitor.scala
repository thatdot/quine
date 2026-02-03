package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.{maybeMatch, maybeMatchList}
import com.thatdot.quine.language.semantic.SemanticToken

object SinglePartQueryVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_SinglePartQuery(ctx: CypherParser.OC_SinglePartQueryContext): List[SemanticToken] = {
    val reading = ctx.oC_ReadingClause().asScala.toList.flatMap(innerCtx => innerCtx.accept(ReadingClauseVisitor))
    val updating = maybeMatchList(ctx.oC_UpdatingClause(), UpdatingClauseVisitor)
    val returnSemantics = maybeMatch(ctx.oC_Return(), ReturnVisitor)

    reading ::: updating.getOrElse(List.empty[List[SemanticToken]]).flatten ::: returnSemantics.getOrElse(
      List.empty[SemanticToken],
    )
  }
}
