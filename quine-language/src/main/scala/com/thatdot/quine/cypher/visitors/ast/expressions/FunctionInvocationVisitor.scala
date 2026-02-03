package com.thatdot.quine.cypher.visitors.ast.expressions

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{Expression, Source}

object FunctionInvocationVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_FunctionInvocation(ctx: CypherParser.OC_FunctionInvocationContext): Expression = {
    val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

    val fname = ctx.oC_FunctionName().getText
    val fargs = ctx.oC_Expression().asScala.toList.map(innerCtx => innerCtx.accept(ExpressionVisitor))

    if (fname == "id") {
      if (fargs.tail.nonEmpty) {
        throw new Exception("Too many arguments given to special `id` function.")
      }
      fargs.head match {
        case ident: Expression.Ident => Expression.IdLookup(src, ident.identifier, None)
        case other => throw new Exception(s"Expected identifier argument to `id` function, got: $other")
      }
    } else if (fname == "idFrom") {
      Expression.SynthesizeId(src, fargs, None)
    } else {
      Expression.Apply(src, Symbol(fname), fargs, None)
    }
  }
}
