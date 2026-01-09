package com.thatdot.quine.docs

import scala.reflect.runtime.universe._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.BuildInfo

class GenerateOpenApiTestV2 extends AnyFunSuite with Matchers {

  test("Main method should be static (i.e., defined in an object)") {
    val className = "com.thatdot.quine.docs.GenerateOpenApiV2"

    try {
      val mirror = runtimeMirror(getClass.getClassLoader)
      val moduleSymbol = mirror.staticModule(className)
      val methodSymbol = moduleSymbol.typeSignature.member(TermName("main"))
      methodSymbol.isMethod shouldBe true
      val method = methodSymbol.asMethod
      method.paramLists.flatten.headOption.exists(_.typeSignature =:= typeOf[Array[String]]) shouldBe true
    } catch {
      case _: ScalaReflectionException =>
        fail(s"$className is not an object or does not have a main method.")
    }
  }

  test("Generated OpenAPI spec should be valid JSON with expected structure") {
    import io.circe.syntax._
    import sttp.apispec.openapi.circe._

    val docs = new GenerateOpenApiV2.QuineOssV2OpenApiDocsImpl
    val api = docs.api

    api.info.title shouldBe "Quine API"
    api.info.version shouldBe BuildInfo.version
    api.paths.pathItems should not be empty

    val json = api.asJson
    json.isObject shouldBe true

    val openApiVersion = json.hcursor.downField("openapi").as[String]
    openApiVersion.isRight shouldBe true
    openApiVersion.toOption.exists(_.startsWith("3.")) shouldBe true
  }

  test("Minimum endpoint count (regression guard)") {
    val docs = new GenerateOpenApiV2.QuineOssV2OpenApiDocsImpl
    val pathCount = docs.api.paths.pathItems.size
    pathCount should be >= 25
  }

  test("No duplicate paths") {
    val docs = new GenerateOpenApiV2.QuineOssV2OpenApiDocsImpl
    val paths = docs.api.paths.pathItems.keys.toSeq
    paths.distinct.size shouldBe paths.size
  }

  test("Hidden endpoints should be excluded") {
    val docs = new GenerateOpenApiV2.QuineOssV2OpenApiDocsImpl
    val visiblePaths = docs.api.paths.pathItems.keys.toSet
    val hiddenPaths = docs.hiddenPaths
    visiblePaths.intersect(hiddenPaths) shouldBe empty
  }
}
