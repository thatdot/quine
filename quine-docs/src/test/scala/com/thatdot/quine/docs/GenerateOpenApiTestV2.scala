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

  test("Experimental Databricks (Delta Sharing) ingest is hidden from the published spec") {
    import io.circe.syntax._
    import sttp.apispec.openapi.circe._

    val docs = new GenerateOpenApiV2.QuineOssV2OpenApiDocsImpl
    val json = docs.api.asJson.spaces2

    // No Databricks/Delta Sharing references anywhere in the document.
    withClue("published OpenAPI must not mention Databricks/Delta Sharing:\n") {
      json should not include "DeltaSharing"
      json should not include "Delta Sharing"
      json should not include "Databricks"
    }

    val schemas = docs.api.components.toList.flatMap(_.schemas.keys)
    // The hidden member and the schemas reachable only through it are gone.
    schemas should contain noneOf ("DeltaSharingCdf", "DeltaSharingAuth", "BearerToken", "OAuthClientCredentials")

    // But the Avro/Parquet file-format support is still advertised.
    schemas should contain("FileFormat")
    json should include("Avro")
    json should include("Parquet")
  }
}
