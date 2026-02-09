package com.thatdot.quine.routes

import endpoints4s.circe.JsonSchemas
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.thatdot.common.security.Secret
import com.thatdot.quine.routes.exts.CirceJsonAnySchema

class AwsSchemaSpec extends AnyWordSpec with Matchers {

  private object TestSchemas extends AwsConfigurationSchemas with JsonSchemas with CirceJsonAnySchema

  "secretSchema encoder" should {
    "redact the value" in {
      val secret = Secret("AKIAIOSFODNN7EXAMPLE")

      TestSchemas.secretSchema.encoder(secret) shouldBe io.circe.Json.fromString("Secret(****)")
    }
  }

  "secretSchema decoder" should {
    "accept plaintext input" in {
      import Secret.Unsafe._
      val json = io.circe.Json.fromString("my-secret-value")
      val decoded = TestSchemas.secretSchema.decoder.decodeJson(json).getOrElse(fail("Failed to decode Secret"))

      decoded.unsafeValue shouldBe "my-secret-value"
    }
  }

  "awsCredentialsSchema encoder" should {
    "redact both credential fields" in {
      val creds = AwsCredentials(
        accessKeyId = Secret("AKIAIOSFODNN7EXAMPLE"),
        secretAccessKey = Secret("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
      )

      TestSchemas.awsCredentialsSchema.encoder(creds) shouldBe io.circe.Json.obj(
        "accessKeyId" -> io.circe.Json.fromString("Secret(****)"),
        "secretAccessKey" -> io.circe.Json.fromString("Secret(****)"),
      )
    }
  }

  "awsCredentialsSchema decoder" should {
    "decode plaintext credentials correctly" in {
      import Secret.Unsafe._
      val json = io.circe.parser
        .parse("""{"accessKeyId": "AKIA123", "secretAccessKey": "secret456"}""")
        .getOrElse(fail("Failed to parse test JSON"))

      val creds =
        TestSchemas.awsCredentialsSchema.decoder.decodeJson(json).getOrElse(fail("Failed to decode AwsCredentials"))
      creds.accessKeyId.unsafeValue shouldBe "AKIA123"
      creds.secretAccessKey.unsafeValue shouldBe "secret456"
    }
  }

  // NOTE: Preserving schema behavior (credential preservation for cluster communication)
  // is tested in V1PreservingCodecsSpec, which tests the actual runtime entry points.
  // We don't test preserving schemas here because:
  // 1. This module (quine-endpoints) shouldn't depend on quine-enterprise
  // 2. Test-scoped schema overrides would only test the mechanism, not the actual runtime code
}
