package com.thatdot.quine.cypher.visitors.ast

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.ast.ReadingClause.{FromProcedure, FromSubquery}
import com.thatdot.quine.cypher.ast.{ReadingClause, YieldItem}
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.visitors.ast.expressions.ExpressionVisitor
import com.thatdot.quine.language.ast.{CypherIdentifier, Source}

object InQueryCallVisitor extends CypherBaseVisitor[Option[ReadingClause]] {
  override def visitOC_InQueryCall(
    ctx: CypherParser.OC_InQueryCallContext,
  ): Option[ReadingClause] = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    if (ctx.oC_Subquery() == null) {
      val exps = ctx
        .oC_ExplicitProcedureInvocation()
        .oC_Expression()
        .asScala
        .toList
        .map(innerCtx => innerCtx.accept(ExpressionVisitor))
      val procName = Symbol(
        ctx.oC_ExplicitProcedureInvocation().oC_ProcedureName().getText,
      )
      val yields = for {
        yieldItems <- Option(ctx.oC_YieldItems())
      } yield yieldItems
        .oC_YieldItem()
        .asScala
        .toList
        .map { innerCtx =>
          // oC_YieldItem: ( oC_ProcedureResultField SP AS SP )? oC_Variable
          // The variable is always the bound name in the query scope
          val boundAsSymbol = Symbol(innerCtx.oC_Variable().getText)
          // If there's a procedure result field (e.g., "result AS alias"), use that
          // Otherwise the result field name is the same as the bound variable
          val resultField = Option(innerCtx.oC_ProcedureResultField())
            .map(field => Symbol(field.getText))
            .getOrElse(boundAsSymbol)
          YieldItem(
            resultField = resultField,
            boundAs = Left(CypherIdentifier(boundAsSymbol)),
          )
        }

      Some(FromProcedure(src, procName, exps, yields.toList.flatten))
    } else {
      val bindings = ctx
        .oC_Subquery()
        .oC_Variable()
        .asScala
        .toList
        .map(innerCtx => Left(CypherIdentifier(Symbol(innerCtx.getText))))
      val maybeSubquery =
        ctx.oC_Subquery().oC_RegularQuery().accept(RegularQueryVisitor)

      for {
        sq <- maybeSubquery
      } yield FromSubquery(src, bindings, sq)
    }
  }
}
