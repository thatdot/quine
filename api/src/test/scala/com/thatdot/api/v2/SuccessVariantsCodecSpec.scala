package com.thatdot.api.v2

import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

/** The success-status discriminator types are *transparent* — `Ok[T]` and `Created[T]`
  * serialize as `T` directly with no wrapping; `NoContent` serializes as an empty body.
  */
class SuccessVariantsCodecSpec extends AnyFunSuite with Matchers with ScalaCheckDrivenPropertyChecks {

  test("Ok[T] encodes as the bare content (no wrapper)") {
    forAll { (s: String) =>
      Ok(s).asJson shouldBe Json.fromString(s)
    }
  }

  test("Ok[T] roundtrips through encode/decode") {
    forAll { (s: String) =>
      val json = Ok(s).asJson
      json.as[Ok[String]] shouldBe Right(Ok(s))
    }
  }

  test("Created[T] encodes as the bare content (no wrapper)") {
    forAll { (n: Int) =>
      Created(n).asJson shouldBe Json.fromInt(n)
    }
  }

  test("Created[T] roundtrips through encode/decode") {
    forAll { (n: Int) =>
      val json = Created(n).asJson
      json.as[Created[Int]] shouldBe Right(Created(n))
    }
  }

  test("NoContent encodes as an empty JSON value") {
    NoContent.asJson shouldBe Json.obj()
  }

  test("NoContent roundtrips") {
    val json = NoContent.asJson
    json.as[NoContent.type] shouldBe Right(NoContent)
  }
}
