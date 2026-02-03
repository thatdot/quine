package com.thatdot.quine.cypher.visitors.ast.expressions

import scala.collection.immutable.Queue
import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime.tree.TerminalNode

import com.thatdot.quine.cypher.parsing.CypherParser.OC_MultiplyDivideModuloExpressionContext
import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.language.ast.{Expression, Operator, Source}

object AddSubtractVisitor extends CypherBaseVisitor[Expression] {

  override def visitOC_AddOrSubtractExpression(ctx: CypherParser.OC_AddOrSubtractExpressionContext): Expression = {
    val children = ctx.children.asScala.toList

    if (children.size == 1) {
      children.head.accept(MultiplyDivideModuloVisitor)
    } else {
      val src = Source.TextSource(start = ctx.start.getStartIndex, end = ctx.stop.getStopIndex)

      val (ops, exps) = ctx.children.asScala.toList.foldLeft(List.empty[Operator] -> Queue.empty[Expression]) {
        (mem, pt) =>
          pt match {
            case mdmec: OC_MultiplyDivideModuloExpressionContext =>
              mem._1 -> mem._2.enqueue(mdmec.accept(MultiplyDivideModuloVisitor))
            case node: TerminalNode =>
              node.getText.trim match {
                case "" => mem
                case "+" => (Operator.Plus :: mem._1) -> mem._2
                case "-" => (Operator.Minus :: mem._1) -> mem._2
                case _ => mem
              }
            case _ => mem
          }
      }

      val (init, rexps) = exps.dequeue

      ops
        .foldLeft(init -> rexps) { case ((e1, rem), op) =>
          val (e2, r2) = rem.dequeue
          Expression.BinOp(src, op, e1, e2, None) -> r2
        }
        ._1
    }
  }
}
