package com.thatdot.quine.cypher.visitors.ast.expressions

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Source}

object PropertyVisitor extends CypherBaseVisitor[Expression.FieldAccess] {
  override def visitOC_PropertyExpression(ctx: CypherParser.OC_PropertyExpressionContext): Expression.FieldAccess =
    ctx.oC_PropertyLookup().asScala.toList match {
      case Nil =>
        // Unreachable for parser-built trees: the grammar requires at least one lookup
        // (oC_PropertyExpression : oC_Atom ( SP? oC_PropertyLookup )+), and a FieldAccess
        // cannot be built without a field. Throwing here is a true grammar-invariant guard at
        // the visitor edge; matching on the list shape keeps the head/tail access total.
        throw new IllegalStateException("Parse error: property expression has no property lookups")

      case firstLookup :: remainingLookups =>
        val initialQualifier: Expression =
          Expression.Ident(Source.NoSource, Left(CypherIdentifier(Symbol(ctx.oC_Atom().oC_Variable().getText))), None)

        // The first property lookup establishes the FieldAccess; the rest nest left-to-right.
        val firstAccess: Expression.FieldAccess = Expression.FieldAccess(
          Source.NoSource,
          initialQualifier,
          Symbol(firstLookup.oC_PropertyKeyName().getText),
          None,
        )

        remainingLookups.foldLeft(firstAccess) { (fieldAccess, lookup) =>
          Expression.FieldAccess(Source.NoSource, fieldAccess, Symbol(lookup.oC_PropertyKeyName().getText), None)
        }
    }
}
