package com.thatdot.api.v2

import io.circe.parser
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ApiErrorsCodecSpec extends AnyFunSuite with Matchers with ScalaCheckDrivenPropertyChecks {
  import ErrorResponseGenerators.Arbs._

  private def errorObj(json: io.circe.Json) = json.hcursor.downField("error")

  test("ServerError encodes AIP-193 envelope: {error: {code:500, status:'INTERNAL', message, details}}") {
    forAll { (e: ErrorResponse.ServerError) =>
      val json = e.asJson
      errorObj(json).get[Int]("code") shouldBe Right(500)
      errorObj(json).get[String]("status") shouldBe Right("INTERNAL")
      errorObj(json).get[String]("message") shouldBe Right(e.message)
      errorObj(json).downField("details").focus.flatMap(_.asArray).map(_.size) shouldBe Some(e.details.size)
    }
  }

  test("BadRequest encodes AIP-193 envelope with code 400 and status INVALID_ARGUMENT") {
    forAll { (e: ErrorResponse.BadRequest) =>
      val json = e.asJson
      errorObj(json).get[Int]("code") shouldBe Right(400)
      errorObj(json).get[String]("status") shouldBe Right("INVALID_ARGUMENT")
      errorObj(json).get[String]("message") shouldBe Right(e.message)
    }
  }

  test("NotFound encodes AIP-193 envelope with code 404 and status NOT_FOUND") {
    forAll { (e: ErrorResponse.NotFound) =>
      val json = e.asJson
      errorObj(json).get[Int]("code") shouldBe Right(404)
      errorObj(json).get[String]("status") shouldBe Right("NOT_FOUND")
    }
  }

  test("Unauthorized encodes AIP-193 envelope with code 401 and status UNAUTHENTICATED") {
    forAll { (e: ErrorResponse.Unauthorized) =>
      val json = e.asJson
      errorObj(json).get[Int]("code") shouldBe Right(401)
      errorObj(json).get[String]("status") shouldBe Right("UNAUTHENTICATED")
    }
  }

  test("ServiceUnavailable encodes AIP-193 envelope with code 503 and status UNAVAILABLE") {
    forAll { (e: ErrorResponse.ServiceUnavailable) =>
      val json = e.asJson
      errorObj(json).get[Int]("code") shouldBe Right(503)
      errorObj(json).get[String]("status") shouldBe Right("UNAVAILABLE")
    }
  }

  test("ErrorDetail variants carry the discriminator type field") {
    (ErrorDetail.Help("h"): ErrorDetail).asJson.hcursor.get[String]("type") shouldBe Right("Help")
    (ErrorDetail.RequestInfo("rid"): ErrorDetail).asJson.hcursor.get[String]("type") shouldBe Right("RequestInfo")
    (ErrorDetail.ErrorInfo("CYPHER_ERROR"): ErrorDetail).asJson.hcursor.get[String]("type") shouldBe Right("ErrorInfo")
  }

  test("cypherError convenience preserves CypherError reason for clients") {
    val br = ErrorResponse.BadRequest("syntax err", List(ErrorDetail.cypherError))
    val json = br.asJson
    val firstDetail = errorObj(json).downField("details").downArray
    firstDetail.get[String]("type") shouldBe Right("ErrorInfo")
    firstDetail.get[String]("reason") shouldBe Right("CYPHER_ERROR")
    firstDetail.get[String]("domain") shouldBe Right("quine.io")
  }

  test("ErrorInfo.metadata round-trips with all entries preserved") {
    val info = ErrorDetail.ErrorInfo(
      reason = "PARSE_ERROR",
      domain = "quine.io",
      metadata = Map("line" -> "42", "column" -> "7", "token" -> "MATCH"),
    )
    val br = ErrorResponse.BadRequest("syntax err at 42:7", List(info))
    // Round-trip through encode → decode → re-extract
    val decoded = parser.parse(br.asJson.noSpaces).flatMap(_.as[ErrorResponse.BadRequest])
    decoded shouldBe Right(br)
    decoded.toOption.get.details.head match {
      case ei: ErrorDetail.ErrorInfo =>
        ei.metadata shouldBe Map("line" -> "42", "column" -> "7", "token" -> "MATCH")
      case other => fail(s"Expected ErrorInfo, got: $other")
    }
  }

  test("ErrorInfo with empty metadata round-trips and encodes the field") {
    val info = ErrorDetail.ErrorInfo("REASON")
    val json = (info: ErrorDetail).asJson
    json.hcursor.downField("metadata").focus.flatMap(_.asObject).map(_.size) shouldBe Some(0)
    json.as[ErrorDetail] shouldBe Right(info)
  }

  test("ServerError round-trips through encode/decode") {
    forAll { (e: ErrorResponse.ServerError) =>
      val decoded = parser.parse(e.asJson.noSpaces).flatMap(_.as[ErrorResponse.ServerError])
      decoded shouldBe Right(e)
    }
  }

  test("BadRequest round-trips through encode/decode") {
    forAll { (e: ErrorResponse.BadRequest) =>
      val decoded = parser.parse(e.asJson.noSpaces).flatMap(_.as[ErrorResponse.BadRequest])
      decoded shouldBe Right(e)
    }
  }
}
