package com.thatdot.quine.cypher.visitors.ast.expressions

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Source}

object PropertyVisitor extends CypherBaseVisitor[Expression.FieldAccess] {
  override def visitOC_PropertyExpression(ctx: CypherParser.OC_PropertyExpressionContext): Expression.FieldAccess = {
    val pls = ctx.oC_PropertyLookup().asScala.toList

    if (pls.isEmpty) {
      // ctx.oC_Atom().accept(AtomVisitor)
      //TODO What needs to happen here?
      ???
    } else {
      val initialQualifier: Expression =
        Expression.Ident(Source.NoSource, Left(CypherIdentifier(Symbol(ctx.oC_Atom().oC_Variable().getText))), None)

      // Handle first property lookup to establish FieldAccess type
      val firstAccess: Expression.FieldAccess = Expression.FieldAccess(
        Source.NoSource,
        initialQualifier,
        Symbol(pls.head.oC_PropertyKeyName().getText),
        None,
      )

      // Fold over remaining property lookups
      pls.tail.foldLeft(firstAccess) { (fa, ctx) =>
        Expression.FieldAccess(Source.NoSource, fa, Symbol(ctx.oC_PropertyKeyName().getText), None)
      }
    }
  }
}
