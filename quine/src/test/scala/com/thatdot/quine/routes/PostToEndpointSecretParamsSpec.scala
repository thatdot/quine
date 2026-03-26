package com.thatdot.quine.routes

import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.common.security.Secret
import com.thatdot.quine.outputs.StandingQueryOutputGenerators.Gens.secretHeaders
import com.thatdot.quine.routes.exts.CirceJsonAnySchema

class PostToEndpointSecretParamsSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  import endpoints4s.circe.JsonSchemas
  private object TestSchemas extends StandingQuerySchemas with JsonSchemas with CirceJsonAnySchema

  private val structure: Gen[StandingQueryOutputStructure] =
    Gen.oneOf(StandingQueryOutputStructure.WithMetadata(), StandingQueryOutputStructure.Bare())

  implicit private val arbPostToEndpoint: Arbitrary[StandingQueryResultOutputUserDef.PostToEndpoint] = Arbitrary(for {
    url <- Gen.alphaNumStr.suchThat(_.nonEmpty).map(s => s"http://localhost:8080/$s")
    parallelism <- Gen.posNum[Int]
    onlyPositiveMatchData <- Arbitrary.arbitrary[Boolean]
    headers <- secretHeaders
    struct <- structure
  } yield StandingQueryResultOutputUserDef.PostToEndpoint(url, parallelism, onlyPositiveMatchData, headers, struct))

  "PostToEndpoint default encoder (API responses)" should {
    "redact header values when present" in {
      forAll { (endpoint: StandingQueryResultOutputUserDef.PostToEndpoint) =>
        whenever(endpoint.headers.nonEmpty) {
          val json = TestSchemas.standingQueryResultOutputSchema.encoder(endpoint)
          val headersJson = json.hcursor.downField("headers")
          endpoint.headers.keys.foreach { key =>
            headersJson.downField(key).as[String] shouldBe Right("Secret(****)")
          }
        }
      }
    }

    "encode empty headers map when no headers provided" in {
      forAll { (endpoint: StandingQueryResultOutputUserDef.PostToEndpoint) =>
        whenever(endpoint.headers.isEmpty) {
          val json = TestSchemas.standingQueryResultOutputSchema.encoder(endpoint)
          val headersJson = json.hcursor.downField("headers")
          headersJson.as[Map[String, String]] shouldBe Right(Map.empty)
        }
      }
    }
  }

  "PostToEndpoint decoder (user input)" should {
    "decode plaintext secrets and wrap them in Secret" in {
      import Secret.Unsafe._

      forAll { (endpoint: StandingQueryResultOutputUserDef.PostToEndpoint) =>
        whenever(endpoint.headers.nonEmpty) {
          val json = PreservingStandingQuerySchemas.standingQueryResultOutputSchema.encoder(endpoint)

          val decoded = TestSchemas.standingQueryResultOutputSchema.decoder
            .decodeJson(json)
            .getOrElse(fail("Failed to decode PostToEndpoint"))

          decoded match {
            case p: StandingQueryResultOutputUserDef.PostToEndpoint =>
              p.url shouldBe endpoint.url
              p.parallelism shouldBe endpoint.parallelism
              p.onlyPositiveMatchData shouldBe endpoint.onlyPositiveMatchData
              p.headers.keys shouldBe endpoint.headers.keys
              endpoint.headers.foreach { case (k, v) =>
                p.headers(k).unsafeValue shouldBe v.unsafeValue
              }
            case other =>
              fail(s"Expected PostToEndpoint but got: $other")
          }
        }
      }
    }
  }

  "PreservingStandingQuerySchemas" should {
    "preserve header secret values" in {
      import Secret.Unsafe._

      forAll { (endpoint: StandingQueryResultOutputUserDef.PostToEndpoint) =>
        whenever(endpoint.headers.nonEmpty) {
          val json = PreservingStandingQuerySchemas.standingQueryResultOutputSchema.encoder(endpoint)
          val headersJson = json.hcursor.downField("headers")
          endpoint.headers.foreach { case (k, v) =>
            headersJson.downField(k).as[String] shouldBe Right(v.unsafeValue)
          }
        }
      }
    }
  }
}
