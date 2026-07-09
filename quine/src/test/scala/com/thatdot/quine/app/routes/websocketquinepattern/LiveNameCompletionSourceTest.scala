package com.thatdot.quine.app.routes.websocketquinepattern

import org.opencypher.v9_0.expressions.functions.Category
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.compiler.cypher.registerUserDefinedFunction
import com.thatdot.quine.graph.cypher.{Type => CypherType, UserDefinedFunction, UserDefinedFunctionSignature, Value}
import com.thatdot.quine.model.QuineIdProvider

class LiveNameCompletionSourceTest extends AnyFunSuite {

  private def labels(): Set[String] = LiveNameCompletionSource.names().map(_.label).toSet

  test("offers Quine's built-in scalar functions and aggregating functions") {
    val names = labels()
    // A scalar built-in from Func.builtinFunctions and an aggregate that is listed explicitly.
    assert(names.contains("abs"), s"expected built-in abs; got a set of size ${names.size}")
    assert(names.contains("count"), "expected the aggregating function count")
    assert(names.contains("collect"), "expected the aggregating function collect")
  }

  test("offers Quine's registered functions and procedures from the live registries") {
    val names = labels()
    // idFrom is registered as a UDF and recentNodes as a procedure when the compiler rewriters
    // initialize; the source forces that initialization, so they are present without compiling a
    // query first.
    assert(names.contains("idFrom"), "expected the Quine function idFrom")
    assert(names.contains("recentNodes"), "expected the Quine procedure recentNodes")
  }

  test("a built-in carries its runtime signature as detail") {
    val abs = LiveNameCompletionSource.names().find(_.label == "abs").getOrElse(fail("expected abs"))
    assert(abs.detail.exists(_.nonEmpty), s"abs should carry a signature; got ${abs.detail}")
  }

  test("offers a user-defined function registered at runtime") {
    registerUserDefinedFunction(LiveCompletionTestUdf)
    assert(labels().contains("liveCompletionTestUdf"), "a UDF registered at runtime is offered as a completion")
  }
}

/** A minimal UDF used only to prove a runtime-registered function reaches completion. */
object LiveCompletionTestUdf extends UserDefinedFunction {
  val name = "liveCompletionTestUdf"
  val isPure = true
  val category: String = Category.SCALAR
  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("input" -> CypherType.Anything),
      output = CypherType.Anything,
      description = "Returns its input unchanged",
    ),
  )
  def call(args: Vector[Value])(implicit idp: QuineIdProvider, logConfig: LogConfig): Value =
    args.head
}
