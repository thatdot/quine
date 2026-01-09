package com.thatdot.quine.v2api

import io.circe.Json
import io.circe.syntax._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.v2api.definitions.QuineIdCodec
import com.thatdot.quine.app.v2api.endpoints.V2CypherEndpointEntities.TCypherQuery
import com.thatdot.quine.graph.{ArbitraryInstances, QuineIdLongProvider}
import com.thatdot.quine.model.QuineIdProvider

class V2CypherCodecSpec
    extends AnyFunSuite
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with ArbitraryInstances
    with QuineIdCodec {
  import V2CypherEndpointGenerators.Arbs.tCypherQuery
  import V2CypherEndpointGenerators.Gens.quineIdFromLong

  private val longProvider = QuineIdLongProvider()
  override lazy val idProvider: QuineIdProvider = longProvider

  test("TCypherQuery roundtrip encoding/decoding preserves data") {
    forAll { (query: TCypherQuery) =>
      val json = query.asJson
      val decoded = json.as[TCypherQuery]
      decoded shouldBe Right(query)
    }
  }

  test("TCypherQuery encodes with correct field names") {
    forAll { (query: TCypherQuery) =>
      val obj = query.asJson.asObject.get
      obj("text").flatMap(_.asString) shouldBe Some(query.text)
      obj("parameters").flatMap(_.asObject).map(_.toMap) shouldBe Some(query.parameters)
    }
  }

  test("TCypherQuery decodes with default parameters when field is omitted") {
    val minimalJson = Json.obj("text" -> Json.fromString("MATCH (n) RETURN n"))
    val decoded = minimalJson.as[TCypherQuery]
    decoded shouldBe Right(TCypherQuery("MATCH (n) RETURN n", Map.empty))
  }

  test("TCypherQuery decodes with explicit empty parameters") {
    val json = Json.obj(
      "text" -> Json.fromString("MATCH (n) RETURN n"),
      "parameters" -> Json.obj(),
    )
    val decoded = json.as[TCypherQuery]
    decoded shouldBe Right(TCypherQuery("MATCH (n) RETURN n", Map.empty))
  }

  test("QuineId roundtrip encoding/decoding preserves data") {
    forAll(quineIdFromLong) { qid =>
      val json = qid.asJson
      val decoded = json.as[QuineId]
      decoded shouldBe Right(qid)
    }
  }

  test("QuineId encodes to string representation") {
    val qid = QuineId(Array[Byte](0, 0, 0, 1))
    val json = qid.asJson
    json.asString.get shouldBe idProvider.qidToPrettyString(qid)
  }

  test("QuineId decodes from valid string representation") {
    val json = Json.fromString("1")
    val decoded = json.as[QuineId]
    idProvider.qidToPrettyString(decoded.getOrElse(fail())) shouldBe "1"
  }

  test("QuineId decoder rejects invalid string for long ID provider") {
    val json = Json.fromString("not-a-valid-id")
    val decoded = json.as[QuineId]
    decoded.isLeft shouldBe true
  }
}
