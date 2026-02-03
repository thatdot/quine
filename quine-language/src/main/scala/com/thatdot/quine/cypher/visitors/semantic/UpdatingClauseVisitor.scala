package com.thatdot.quine.cypher.visitors.semantic

import cats.implicits._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.{maybeMatch, maybeMatchList, requireOne}
import com.thatdot.quine.language.semantic.SemanticToken

object UpdatingClauseVisitor extends CypherBaseVisitor[List[SemanticToken]] {
  override def visitOC_UpdatingClause(ctx: CypherParser.OC_UpdatingClauseContext): List[SemanticToken] = {

    val maybeSets = (for {
      set <- Option(ctx.oC_Effect().oC_Set())
      items <- maybeMatchList(set.oC_SetItem(), SetItemVisitor)
    } yield items).map(_.flatten)

    val maybeCreate = maybeMatch(ctx.oC_Effect().oC_Create(), CreateVisitor)

    requireOne(maybeSets <+> maybeCreate, "updating clause")
  }
}
