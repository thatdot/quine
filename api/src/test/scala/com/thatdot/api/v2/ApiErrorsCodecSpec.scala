package com.thatdot.api.v2

import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ApiErrorsCodecSpec extends AnyFunSuite with Matchers with ScalaCheckDrivenPropertyChecks {
  import ErrorTypeGenerators.Arbs._
  import ErrorResponseGenerators.Arbs._

  test("ErrorType.ApiError encodes message field") {
    forAll { (error: ErrorType) =>
      val json = error.asJson
      json.hcursor.get[String]("message") shouldBe Right(error.message)
    }
  }

  test("ErrorType.DecodeError encodes optional help field") {
    forAll { (error: ErrorType.DecodeError) =>
      val json = error.asJson
      error.help match {
        case Some(h) => json.hcursor.get[String]("help") shouldBe Right(h)
        case None => json.hcursor.get[String]("help").isLeft shouldBe true
      }
    }
  }

  test("ErrorType encodes with type discriminator") {
    (ErrorType.ApiError("msg"): ErrorType).asJson.hcursor.get[String]("type") shouldBe Right("ApiError")
    (ErrorType.DecodeError("msg"): ErrorType).asJson.hcursor.get[String]("type") shouldBe Right("DecodeError")
    (ErrorType.CypherError("msg"): ErrorType).asJson.hcursor.get[String]("type") shouldBe Right("CypherError")
  }

  test("ErrorResponse.ServerError encodes errors list") {
    forAll { (error: ErrorResponse.ServerError) =>
      val json = error.asJson
      val errorsArray = json.hcursor.downField("errors").focus.flatMap(_.asArray)
      errorsArray.isDefined shouldBe true
      errorsArray.get.size shouldBe error.errors.size
    }
  }

  test("ErrorResponse.BadRequest encodes errors list") {
    forAll { (error: ErrorResponse.BadRequest) =>
      val json = error.asJson
      val errorsArray = json.hcursor.downField("errors").focus.flatMap(_.asArray)
      errorsArray.get.size shouldBe error.errors.size
    }
  }

  test("ErrorResponse.NotFound encodes errors list") {
    forAll { (error: ErrorResponse.NotFound) =>
      val json = error.asJson
      val errorsArray = json.hcursor.downField("errors").focus.flatMap(_.asArray)
      errorsArray.get.size shouldBe error.errors.size
    }
  }

  test("ErrorResponse.Unauthorized encodes errors list") {
    forAll { (error: ErrorResponse.Unauthorized) =>
      val json = error.asJson
      val errorsArray = json.hcursor.downField("errors").focus.flatMap(_.asArray)
      errorsArray.get.size shouldBe error.errors.size
    }
  }

  test("ErrorResponse.ServiceUnavailable encodes errors list") {
    forAll { (error: ErrorResponse.ServiceUnavailable) =>
      val json = error.asJson
      val errorsArray = json.hcursor.downField("errors").focus.flatMap(_.asArray)
      errorsArray.get.size shouldBe error.errors.size
    }
  }

  test("ErrorResponse types preserve error content when encoded") {
    val errorList = List(ErrorType.ApiError("error1"), ErrorType.CypherError("error2"))
    val serverError = ErrorResponse.ServerError(errorList)
    val json = serverError.asJson

    val errorsArray = json.hcursor.downField("errors").focus.flatMap(_.asArray).get
    errorsArray.size shouldBe 2
    errorsArray.head.hcursor.get[String]("message") shouldBe Right("error1")
    errorsArray.head.hcursor.get[String]("type") shouldBe Right("ApiError")
    errorsArray(1).hcursor.get[String]("message") shouldBe Right("error2")
    errorsArray(1).hcursor.get[String]("type") shouldBe Right("CypherError")
  }
}
