package com.thatdot.quine.v2api

import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.app.v2api.definitions.QuineIdCodec
import com.thatdot.quine.app.v2api.endpoints.V2CypherEndpointEntities.{
  TCypherQuery,
  TCypherQueryResult,
  TUiEdge,
  TUiNode,
}
import com.thatdot.quine.graph.QuineIdLongProvider
import com.thatdot.quine.model.QuineIdProvider

class V2CypherEndpointCodecSpec
    extends AnyFunSuite
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with QuineIdCodec {
  import V2CypherEndpointGenerators.Arbs._

  private val longProvider: QuineIdLongProvider = QuineIdLongProvider()
  override lazy val idProvider: QuineIdProvider = longProvider

  test("TCypherQuery roundtrip encoding/decoding") {
    forAll { (query: TCypherQuery) =>
      val json = query.asJson
      val decoded = json.as[TCypherQuery]
      decoded shouldBe Right(query)
    }
  }

  test("TCypherQuery encodes with correct field names") {
    forAll { (query: TCypherQuery) =>
      val json = query.asJson
      val obj = json.asObject.get
      obj("text").flatMap(_.asString) shouldBe Some(query.text)
      obj("parameters").flatMap(_.asObject).map(_.toMap) shouldBe Some(query.parameters)
    }
  }

  test("TCypherQuery decodes with default parameters when omitted") {
    val minimalJson = Json.obj("text" -> Json.fromString("MATCH (n) RETURN n"))
    val decoded = minimalJson.as[TCypherQuery]
    decoded shouldBe Right(TCypherQuery("MATCH (n) RETURN n", Map.empty))
  }

  test("TCypherQueryResult property-based encoding produces valid JSON objects") {
    forAll { (result: TCypherQueryResult) =>
      val json = result.asJson
      val obj = json.asObject.get
      obj("columns").flatMap(_.asArray).map(_.flatMap(_.asString)) shouldBe Some(result.columns)
      obj("results").flatMap(_.asArray).map(_.size) shouldBe Some(result.results.size)
    }
  }

  test("TUiNode property-based encoding produces valid JSON objects") {
    forAll { (node: TUiNode) =>
      val json = node.asJson
      val obj = json.asObject.get
      obj("id").flatMap(_.asString) shouldBe defined
      obj("hostIndex").flatMap(_.asNumber).flatMap(_.toInt) shouldBe Some(node.hostIndex)
      obj("label").flatMap(_.asString) shouldBe Some(node.label)
      obj("properties").flatMap(_.asObject).map(_.toMap) shouldBe Some(node.properties)
    }
  }

  test("TUiEdge property-based encoding produces valid JSON objects") {
    forAll { (edge: TUiEdge) =>
      val json = edge.asJson
      val obj = json.asObject.get
      obj("from").flatMap(_.asString) shouldBe defined
      obj("edgeType").flatMap(_.asString) shouldBe Some(edge.edgeType)
      obj("to").flatMap(_.asString) shouldBe defined
      obj("isDirected").flatMap(_.asBoolean) shouldBe Some(edge.isDirected)
    }
  }

  test("TUiEdge encodes with default isDirected when true") {
    val edge = TUiEdge(
      from = longProvider.customIdToQid(1.toLong),
      edgeType = "KNOWS",
      to = longProvider.customIdToQid(2.toLong),
    )
    val json = edge.asJson
    val obj = json.asObject.get
    obj("isDirected").flatMap(_.asBoolean) shouldBe Some(true)
  }
}
