package com.thatdot.quine.cypher.utils

import scala.jdk.CollectionConverters._

import cats.implicits._
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor

object Helpers {

  /** Extracts a value from an Option, throwing a descriptive exception if None.
    * Use this instead of .get to provide better error messages during parsing.
    */
  def requireOne[A](combined: Option[A], context: String): A =
    combined.getOrElse(
      throw new IllegalStateException(s"Parse error: no valid alternative found for $context"),
    )
  def maybeMatch[A](ctx: ParserRuleContext, visitor: AbstractParseTreeVisitor[A]): Option[A] =
    if (ctx == null) Option.empty[A] else Some(ctx.accept(visitor))

  def maybeMatchList[A, B <: ParserRuleContext](
    ctx: java.util.List[B],
    visitor: AbstractParseTreeVisitor[A],
  ): Option[List[A]] =
    if (ctx == null) Option.empty[List[A]] else ctx.asScala.toList.traverse(inner => maybeMatch(inner, visitor))
}
