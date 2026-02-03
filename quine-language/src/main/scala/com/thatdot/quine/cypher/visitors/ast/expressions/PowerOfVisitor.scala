package com.thatdot.quine.cypher.visitors.ast.expressions

import scala.collection.immutable.Queue
import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime.tree.TerminalNode

import com.thatdot.quine.cypher.parsing.CypherParser.OC_UnaryAddOrSubtractExpressionContext
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{Expression, Operator, Source}

object PowerOfVisitor extends CypherBaseVisitor[Expression] {
  override def visitOC_PowerOfExpression(ctx: CypherParser.OC_PowerOfExpressionContext): Expression = {
    val (ops, exps) = ctx.children.asScala.toList.foldLeft(List.empty[Operator] -> Queue.empty[Expression]) {
      (mem, pt) =>
        pt match {
          case uas: OC_UnaryAddOrSubtractExpressionContext =>
            mem._1 -> mem._2.enqueue(uas.accept(UnaryAddSubtractVisitor))
          case node: TerminalNode =>
            node.getText.trim match {
              case "" => mem
              case "^" => (Operator.Carat :: mem._1) -> mem._2
              case _ => mem
            }
          case _ => mem
        }
    }

    val (init, rexps) = exps.dequeue

    ops
      .foldLeft(init -> rexps) { case ((e1, rem), op) =>
        val (e2, r2) = rem.dequeue
        Expression.BinOp(Source.NoSource, op, e1, e2, None) -> r2
      }
      ._1
  }
}
