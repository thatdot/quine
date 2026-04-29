package com.thatdot.api.v2

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.api.v2.ShowShort.syntax._

class ShowShortSpec extends AnyFunSuite with Matchers {

  test("HasError with no details renders as [message]") {
    ErrorResponse.NotFound("missing").showShort shouldBe "[missing]"
  }

  test("HasError with only RequestInfo renders the correlation id inline") {
    val e = ErrorResponse.ServerError("internal", List(ErrorDetail.RequestInfo("abc-123")))
    e.showShort shouldBe "[internal; requestId=abc-123]"
  }

  test("HasError with only ErrorInfo renders the reason inline") {
    val e = ErrorResponse.BadRequest("syntax err", List(ErrorDetail.cypherError))
    e.showShort shouldBe "[syntax err; reason=CYPHER_ERROR]"
  }

  test("HasError with only Help summarizes as a count") {
    val e = ErrorResponse.BadRequest("decode fail", List(ErrorDetail.Help("hint1"), ErrorDetail.Help("hint2")))
    e.showShort shouldBe "[decode fail (+2 help)]"
  }

  test("HasError with structured + Help mixes inline render and count") {
    val e = ErrorResponse.ServerError(
      "boom",
      List(ErrorDetail.RequestInfo("uuid"), ErrorDetail.Help("hint"), ErrorDetail.cypherError),
    )
    e.showShort shouldBe "[boom; requestId=uuid, reason=CYPHER_ERROR (+1 help)]"
  }

  test("HasError with multiple structured details joins with comma") {
    val e = ErrorResponse.BadRequest(
      "bad",
      List(ErrorDetail.RequestInfo("u"), ErrorDetail.cypherError),
    )
    e.showShort shouldBe "[bad; requestId=u, reason=CYPHER_ERROR]"
  }
}
