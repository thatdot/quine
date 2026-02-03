package com.thatdot.quine.cypher.visitors.ast.expressions

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{Expression, Source}

object NonArithmeticOperatorVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_NonArithmeticOperatorExpression(
    ctx: CypherParser.OC_NonArithmeticOperatorExpressionContext,
  ): Expression = {
    val arrayLookup = ctx.oC_ListOperatorExpression().asScala

    val pls = ctx.oC_PropertyLookup().asScala

    val init = ctx.oC_Atom().accept(AtomVisitor)

    if (pls.isEmpty && arrayLookup.isEmpty) {
      init
    } else if (pls.nonEmpty) {
      pls.foldLeft(init) { (of, innerCtx) =>
        val fieldSrc = Source.TextSource(start = innerCtx.start.getStartIndex, end = innerCtx.stop.getStopIndex)
        val fieldName = Symbol(innerCtx.oC_PropertyKeyName().getText)
        Expression.FieldAccess(fieldSrc, of, fieldName, None)
      }
    } else { // pls.isEmpty && arrayLookup.nonEmpty
      arrayLookup.foldLeft(init) { (of, innerCtx) =>
        val arraySrc = Source.TextSource(start = innerCtx.start.getStartIndex, end = innerCtx.stop.getStopIndex)

        val indexExp = innerCtx.oC_Expression(0).accept(ExpressionVisitor)
        Expression.IndexIntoArray(arraySrc, of, indexExp, None)
      }
    }
  }
}
