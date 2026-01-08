package com.thatdot.api.v2

import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class SuccessEnvelopeCodecSpec extends AnyFunSuite with Matchers with ScalaCheckDrivenPropertyChecks {
  import SuccessEnvelopeGenerators.Arbs._

  test("SuccessEnvelope.Ok encodes content field") {
    forAll { (envelope: SuccessEnvelope.Ok[String]) =>
      val json = envelope.asJson
      json.hcursor.get[String]("content") shouldBe Right(envelope.content)
    }
  }

  test("SuccessEnvelope.Ok encodes optional message field") {
    forAll { (envelope: SuccessEnvelope.Ok[String]) =>
      val json = envelope.asJson
      envelope.message match {
        case Some(msg) => json.hcursor.get[String]("message") shouldBe Right(msg)
        case None => json.hcursor.get[Option[String]]("message") shouldBe Right(None)
      }
    }
  }

  test("SuccessEnvelope.Ok encodes warnings list") {
    forAll { (envelope: SuccessEnvelope.Ok[String]) =>
      val json = envelope.asJson
      json.hcursor.get[List[String]]("warnings") shouldBe Right(envelope.warnings)
    }
  }

  test("SuccessEnvelope.Ok roundtrips encode/decode") {
    forAll { (envelope: SuccessEnvelope.Ok[String]) =>
      val json = envelope.asJson
      val decoded = json.as[SuccessEnvelope.Ok[String]]
      decoded shouldBe Right(envelope)
    }
  }

  test("SuccessEnvelope.Created encodes content field") {
    forAll { (envelope: SuccessEnvelope.Created[String]) =>
      val json = envelope.asJson
      json.hcursor.get[String]("content") shouldBe Right(envelope.content)
    }
  }

  test("SuccessEnvelope.Created encodes optional message field") {
    forAll { (envelope: SuccessEnvelope.Created[String]) =>
      val json = envelope.asJson
      envelope.message match {
        case Some(msg) => json.hcursor.get[String]("message") shouldBe Right(msg)
        case None => json.hcursor.get[Option[String]]("message") shouldBe Right(None)
      }
    }
  }

  test("SuccessEnvelope.Created encodes warnings list") {
    forAll { (envelope: SuccessEnvelope.Created[String]) =>
      val json = envelope.asJson
      json.hcursor.get[List[String]]("warnings") shouldBe Right(envelope.warnings)
    }
  }

  test("SuccessEnvelope.Created roundtrips encode/decode") {
    forAll { (envelope: SuccessEnvelope.Created[String]) =>
      val json = envelope.asJson
      val decoded = json.as[SuccessEnvelope.Created[String]]
      decoded shouldBe Right(envelope)
    }
  }

  test("SuccessEnvelope.Accepted encodes message field") {
    forAll { (envelope: SuccessEnvelope.Accepted) =>
      val json = envelope.asJson
      json.hcursor.get[String]("message") shouldBe Right(envelope.message)
    }
  }

  test("SuccessEnvelope.Accepted encodes optional monitorUrl field") {
    forAll { (envelope: SuccessEnvelope.Accepted) =>
      val json = envelope.asJson
      envelope.monitorUrl match {
        case Some(url) => json.hcursor.get[String]("monitorUrl") shouldBe Right(url)
        case None => json.hcursor.get[Option[String]]("monitorUrl") shouldBe Right(None)
      }
    }
  }

  test("SuccessEnvelope.Accepted roundtrips encode/decode") {
    forAll { (envelope: SuccessEnvelope.Accepted) =>
      val json = envelope.asJson
      val decoded = json.as[SuccessEnvelope.Accepted]
      decoded shouldBe Right(envelope)
    }
  }

  test("SuccessEnvelope.NoContent encodes to unit-like JSON") {
    val json = SuccessEnvelope.NoContent.asJson
    // NoContent is encoded as unit, which is an empty object
    json shouldBe io.circe.Json.obj()
  }

  test("SuccessEnvelope.NoContent roundtrips encode/decode") {
    val json = SuccessEnvelope.NoContent.asJson
    val decoded = json.as[SuccessEnvelope.NoContent.type]
    decoded shouldBe Right(SuccessEnvelope.NoContent)
  }

  test("SuccessEnvelope.Ok works with Int content") {
    forAll { (envelope: SuccessEnvelope.Ok[Int]) =>
      val json = envelope.asJson
      json.hcursor.get[Int]("content") shouldBe Right(envelope.content)
      json.as[SuccessEnvelope.Ok[Int]] shouldBe Right(envelope)
    }
  }

  test("SuccessEnvelope.Ok works with List[String] content") {
    forAll { (envelope: SuccessEnvelope.Ok[List[String]]) =>
      val json = envelope.asJson
      json.as[SuccessEnvelope.Ok[List[String]]] shouldBe Right(envelope)
    }
  }

  test("SuccessEnvelope.Ok encoder outputs all fields including defaults") {
    val envelope = SuccessEnvelope.Ok("test", None, Nil)
    val json = envelope.asJson
    json.hcursor.get[String]("content") shouldBe Right("test")
    json.hcursor.get[Option[String]]("message") shouldBe Right(None)
    json.hcursor.get[List[String]]("warnings") shouldBe Right(Nil)
  }

  test("SuccessEnvelope.Created encoder outputs all fields including defaults") {
    val envelope = SuccessEnvelope.Created("test", None, Nil)
    val json = envelope.asJson
    json.hcursor.get[String]("content") shouldBe Right("test")
    json.hcursor.get[Option[String]]("message") shouldBe Right(None)
    json.hcursor.get[List[String]]("warnings") shouldBe Right(Nil)
  }

  test("SuccessEnvelope.Accepted decodes from minimal JSON with defaults applied") {
    val minimalJson = io.circe.Json.obj()
    val decoded = minimalJson.as[SuccessEnvelope.Accepted]
    decoded shouldBe Right(SuccessEnvelope.Accepted())
  }
}
