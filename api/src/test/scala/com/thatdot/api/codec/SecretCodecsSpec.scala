package com.thatdot.api.codec

import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.thatdot.api.codec.SecretCodecs._
import com.thatdot.common.security.Secret

class SecretCodecsSpec extends AnyWordSpec with Matchers {

  "secretEncoder" should {
    "redact the actual value" in {
      val secret = Secret("AKIAIOSFODNN7EXAMPLE")

      secret.asJson shouldBe Json.fromString("Secret(****)")
    }
  }

  "secretDecoder" should {
    "wrap incoming string and preserve value internally" in {
      import Secret.Unsafe._
      val originalValue = "my-secret-value"
      val json = Json.fromString(originalValue)

      val decoded = json.as[Secret].getOrElse(fail("Failed to decode Secret"))

      decoded.toString shouldBe "Secret(****)"
      decoded.unsafeValue shouldBe originalValue
    }
  }

  "preservingEncoder" should {
    "preserve actual credential value" in {
      import Secret.Unsafe._
      val value = "real-credential-value"

      Secret(value).asJson(preservingEncoder) shouldBe Json.fromString(value)
    }

    "produce different output than standard encoder" in {
      import Secret.Unsafe._
      val secret = Secret("credential")

      secret.asJson(secretEncoder) shouldNot be(secret.asJson(preservingEncoder))
    }

    "preserve value through roundtrip" in {
      import Secret.Unsafe._

      val originalValue = "AKIAIOSFODNN7EXAMPLE"
      val secret = Secret(originalValue)
      val json = secret.asJson(preservingEncoder)

      val decoded = json.as[Secret].getOrElse(fail("Failed to decode Secret"))

      decoded.unsafeValue shouldBe originalValue
    }
  }
}
