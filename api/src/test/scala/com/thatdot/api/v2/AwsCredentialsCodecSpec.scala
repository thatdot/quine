package com.thatdot.api.v2

import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.common.security.Secret

class AwsCredentialsCodecSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  import AwsGenerators.Arbs._

  "AwsCredentials encoder" should {
    "redact credentials in JSON output" in {
      val creds = AwsCredentials(
        accessKeyId = Secret("AKIAIOSFODNN7EXAMPLE"),
        secretAccessKey = Secret("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
      )

      creds.asJson shouldBe Json.obj(
        "accessKeyId" -> Json.fromString("Secret(****)"),
        "secretAccessKey" -> Json.fromString("Secret(****)"),
      )
    }
  }

  "AwsCredentials decoder" should {
    "decode JSON with plain strings" in {
      import Secret.Unsafe._
      val json = Json.obj(
        "accessKeyId" -> Json.fromString("AKIATEST"),
        "secretAccessKey" -> Json.fromString("secretkey123"),
      )
      val creds = json.as[AwsCredentials].getOrElse(fail("Failed to decode AwsCredentials"))
      creds.accessKeyId.unsafeValue shouldBe "AKIATEST"
      creds.secretAccessKey.unsafeValue shouldBe "secretkey123"
    }

    "decode values correctly for any credentials (property-based)" in {
      import Secret.Unsafe._
      forAll { (creds: AwsCredentials) =>
        val originalAccessKey = creds.accessKeyId.unsafeValue
        val originalSecretKey = creds.secretAccessKey.unsafeValue

        val inputJson = Json.obj(
          "accessKeyId" -> Json.fromString(originalAccessKey),
          "secretAccessKey" -> Json.fromString(originalSecretKey),
        )

        val decoded = inputJson.as[AwsCredentials].getOrElse(fail("Failed to decode AwsCredentials"))
        decoded.accessKeyId.unsafeValue shouldBe originalAccessKey
        decoded.secretAccessKey.unsafeValue shouldBe originalSecretKey
      }
    }
  }

  "AwsCredentials.preservingEncoder" should {
    "preserve credential values in JSON output" in {
      import Secret.Unsafe._
      val accessKey = "AKIAIOSFODNN8EXAMPLE"
      val secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
      val creds = AwsCredentials(
        accessKeyId = Secret(accessKey),
        secretAccessKey = Secret(secretKey),
      )

      val json = creds.asJson(AwsCredentials.preservingEncoder)

      json shouldBe Json.obj(
        "accessKeyId" -> Json.fromString(accessKey),
        "secretAccessKey" -> Json.fromString(secretKey),
      )
    }

    "produce different output than standard encoder" in {
      import Secret.Unsafe._
      val accessKey = "AKIA123"
      val secretKey = "secret456"
      val creds = AwsCredentials(
        accessKeyId = Secret(accessKey),
        secretAccessKey = Secret(secretKey),
      )

      val redacted = creds.asJson
      val preserved = creds.asJson(AwsCredentials.preservingEncoder)

      redacted.hcursor.downField("accessKeyId") shouldNot be(preserved.hcursor.downField("accessKeyId"))
      redacted.hcursor.downField("secretAccessKey") shouldNot be(preserved.hcursor.downField("secretAccessKey"))
    }

    "preserve values through roundtrip (property-based)" in {
      import Secret.Unsafe._
      forAll { (creds: AwsCredentials) =>
        val originalAccessKey = creds.accessKeyId.unsafeValue
        val originalSecretKey = creds.secretAccessKey.unsafeValue

        val json = creds.asJson(AwsCredentials.preservingEncoder)
        val decoded = json.as[AwsCredentials].getOrElse(fail("Failed to decode AwsCredentials"))
        decoded.accessKeyId.unsafeValue shouldBe originalAccessKey
        decoded.secretAccessKey.unsafeValue shouldBe originalSecretKey
      }
    }
  }
}
