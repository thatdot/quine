package com.thatdot.quine.docs

import scala.reflect.runtime.universe._

import org.scalatest.funsuite.AnyFunSuite

class GenerateOpenApiTest extends AnyFunSuite {

  test("Main method should be static (i.e., defined in an object)") {
    val className = "com.thatdot.quine.docs.GenerateOpenApi"

    try {
      // Attempt to get the companion object (singleton object) of the main class
      val mirror = runtimeMirror(getClass.getClassLoader)
      val moduleSymbol = mirror.staticModule(className)

      // Check if the 'main' method exists
      val methodSymbol = moduleSymbol.typeSignature.member(TermName("main"))
      assert(methodSymbol.isMethod, "Main method should exist in the object")

      // Optionally, check if the method has the correct signature
      val method = methodSymbol.asMethod
      assert(
        method.paramLists.flatten.headOption.exists(_.typeSignature =:= typeOf[Array[String]]),
        "Main method should accept Array[String] as argument",
      )

    } catch {
      case _: ScalaReflectionException =>
        fail(s"$className is not an object or does not have a main method.")
    }
  }
}
