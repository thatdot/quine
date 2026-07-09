package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.semantic.SemanticToken

object MultiPartQueryVisitor extends CypherBaseVisitor[List[SemanticToken]] {

  /** Emits the tokens of every clause of a multipart query in document order. The grammar
    * interleaves reading clauses, updating clauses, and WITH clauses
    * (`( (oC_ReadingClause SP?)* (oC_UpdatingClause SP?)* oC_With SP? )+ oC_SinglePartQuery`),
    * so the children are walked as parsed instead of grouping them by rule.
    */
  override def visitOC_MultiPartQuery(ctx: CypherParser.OC_MultiPartQueryContext): List[SemanticToken] =
    ctx.children.asScala.toList.flatMap {
      case reading: CypherParser.OC_ReadingClauseContext => reading.accept(ReadingClauseVisitor)
      case updating: CypherParser.OC_UpdatingClauseContext => updating.accept(UpdatingClauseVisitor)
      case withClause: CypherParser.OC_WithContext => withClause.accept(WithClauseVisitor)
      case singlePart: CypherParser.OC_SinglePartQueryContext => singlePart.accept(SinglePartQueryVisitor)
      case _ => Nil // whitespace (SP) terminals carry no semantic tokens
    }
}
