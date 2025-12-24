package com.thatdot.quine.v2api

import io.circe.syntax._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQuery._
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern._
import com.thatdot.quine.app.v2api.definitions.query.standing.{StandingQueryPattern, StandingQueryStats}

class V2StandingEndpointCodecSpec extends AnyFunSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  import V2StandingEndpointGenerators.Arbs._

  describe("StandingQueryStats codec") {
    it("should roundtrip encode/decode") {
      forAll { (stats: StandingQueryStats) =>
        val json = stats.asJson
        val decoded = json.as[StandingQueryStats]
        decoded shouldBe Right(stats)
      }
    }

    it("should encode with correct field names") {
      forAll { (stats: StandingQueryStats) =>
        val json = stats.asJson
        json.hcursor.downField("rates").succeeded shouldBe true
        json.hcursor.downField("startTime").succeeded shouldBe true
        json.hcursor.downField("totalRuntime").as[Long] shouldBe Right(stats.totalRuntime)
        json.hcursor.downField("bufferSize").as[Int] shouldBe Right(stats.bufferSize)
      }
    }
  }

  describe("StandingQueryMode codec") {
    it("should roundtrip encode/decode") {
      forAll { (mode: StandingQueryMode) =>
        val json = mode.asJson
        val decoded = json.as[StandingQueryMode]
        decoded shouldBe Right(mode)
      }
    }

    it("should encode as simple string (enumeration style)") {
      forAll { (mode: StandingQueryMode) =>
        val json = mode.asJson
        val expectedValue = mode.getClass.getSimpleName.stripSuffix("$")
        json.as[String] shouldBe Right(expectedValue)
      }
    }
  }

  describe("StandingQueryPattern codec") {
    it("should roundtrip encode/decode") {
      forAll { (pattern: StandingQueryPattern) =>
        val json = pattern.asJson
        val decoded = json.as[StandingQueryPattern]
        decoded shouldBe Right(pattern)
      }
    }

    it("should include type discriminator") {
      forAll { (pattern: StandingQueryPattern) =>
        val json = pattern.asJson
        val expectedType = pattern.getClass.getSimpleName.stripSuffix("$")
        json.hcursor.downField("type").as[String] shouldBe Right(expectedType)
      }
    }
  }

  describe("Cypher codec") {
    it("should roundtrip encode/decode") {
      forAll { (cypher: Cypher) =>
        val json = cypher.asInstanceOf[StandingQueryPattern].asJson
        val decoded = json.as[StandingQueryPattern]
        decoded shouldBe Right(cypher)
      }
    }

    it("should encode query field correctly") {
      forAll { (cypher: Cypher) =>
        val json = cypher.asInstanceOf[StandingQueryPattern].asJson
        json.hcursor.downField("query").as[String] shouldBe Right(cypher.query)
      }
    }
  }

  describe("StandingQueryDefinition codec") {
    it("should roundtrip encode/decode") {
      forAll { (definition: StandingQueryDefinition) =>
        val json = definition.asJson
        val decoded = json.as[StandingQueryDefinition]
        decoded shouldBe Right(definition)
      }
    }

    it("should encode with correct field names") {
      forAll { (definition: StandingQueryDefinition) =>
        val json = definition.asJson
        json.hcursor.downField("name").as[String] shouldBe Right(definition.name)
        json.hcursor.downField("pattern").succeeded shouldBe true
        json.hcursor.downField("includeCancellations").as[Boolean] shouldBe Right(definition.includeCancellations)
        json.hcursor.downField("inputBufferSize").as[Int] shouldBe Right(definition.inputBufferSize)
      }
    }
  }

  describe("RegisteredStandingQuery codec") {
    it("should roundtrip encode/decode") {
      forAll { (rsq: RegisteredStandingQuery) =>
        val json = rsq.asJson
        val decoded = json.as[RegisteredStandingQuery]
        decoded shouldBe Right(rsq)
      }
    }

    it("should encode with correct field names") {
      forAll { (rsq: RegisteredStandingQuery) =>
        val json = rsq.asJson
        json.hcursor.downField("name").as[String] shouldBe Right(rsq.name)
        json.hcursor.downField("internalId").succeeded shouldBe true
        json.hcursor.downField("includeCancellations").as[Boolean] shouldBe Right(rsq.includeCancellations)
        json.hcursor.downField("stats").succeeded shouldBe true
      }
    }
  }
}
