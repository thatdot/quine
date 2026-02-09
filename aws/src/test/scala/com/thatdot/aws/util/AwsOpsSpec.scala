package com.thatdot.aws.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider

import com.thatdot.aws.model.AwsCredentials
import com.thatdot.common.security.Secret

class AwsOpsSpec extends AnyWordSpec with Matchers {

  "staticCredentialsProviderV2" should {
    "extract actual Secret values for SDK usage" in {
      val accessKeyId = "AKIAIOSFODNN7EXAMPLE"
      val secretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

      val credentials = AwsCredentials(
        accessKeyId = Secret(accessKeyId),
        secretAccessKey = Secret(secretAccessKey),
      )

      val provider = AwsOps.staticCredentialsProviderV2(Some(credentials))
      val resolved = provider.resolveCredentials()
      resolved.accessKeyId() shouldBe accessKeyId
      resolved.secretAccessKey() shouldBe secretAccessKey
    }

    "return DefaultCredentialsProvider when credentials are None" in {
      val provider = AwsOps.staticCredentialsProviderV2(None)
      provider shouldBe a[DefaultCredentialsProvider]
    }

    "preserve credential values through Secret wrapper" in {
      val testCases = Seq(
        ("AKIA123", "secret123"),
        ("AKIASPECIAL!@#$%", "secret/with+special=chars"),
        ("A" * 20, "B" * 40),
      )

      for ((accessKey, secretKey) <- testCases) {
        val credentials = AwsCredentials(Secret(accessKey), Secret(secretKey))
        val provider = AwsOps.staticCredentialsProviderV2(Some(credentials))
        val resolved = provider.resolveCredentials()

        withClue(s"For accessKey=$accessKey, secretKey=$secretKey: ") {
          resolved.accessKeyId() shouldBe accessKey
          resolved.secretAccessKey() shouldBe secretKey
        }
      }
    }
  }
}
