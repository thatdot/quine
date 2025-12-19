package com.thatdot.quine.v2api

import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.ArbitraryCommon
import com.thatdot.quine.app.v2api.endpoints.V2DebugEndpointEntities.{TEdgeDirection, TLiteralNode, TRestHalfEdge}

class V2DebugEndpointCodecSpec
    extends AnyFunSuite
    with Matchers
    with ScalaCheckDrivenPropertyChecks
    with V2DebugEndpointCodecSpecGenerators {

  test("TEdgeDirection.Outgoing encodes correctly") {
    val json = (TEdgeDirection.Outgoing: TEdgeDirection).asJson
    json.asString shouldBe Some("Outgoing")
  }

  test("TEdgeDirection.Incoming encodes correctly") {
    val json = (TEdgeDirection.Incoming: TEdgeDirection).asJson
    json.asString shouldBe Some("Incoming")
  }

  test("TEdgeDirection.Undirected encodes correctly") {
    val json = (TEdgeDirection.Undirected: TEdgeDirection).asJson
    json.asString shouldBe Some("Undirected")
  }

  test("TEdgeDirection property-based encoding produces expected strings") {
    forAll { (direction: TEdgeDirection) =>
      val json = direction.asJson
      val str = json.asString
      str.get shouldBe direction.toString
    }
  }

  test("TRestHalfEdge property-based encoding produces valid JSON objects") {
    forAll { (halfEdge: TRestHalfEdge[String]) =>
      val json = halfEdge.asJson
      val obj = json.asObject.get
      obj("direction").flatMap(_.asString) shouldBe Some(halfEdge.direction.toString)
      obj("edgeType").flatMap(_.asString) shouldBe Some(halfEdge.edgeType)
      obj("other").flatMap(_.asString) shouldBe Some(halfEdge.other)
    }
  }

  test("TLiteralNode property-based encoding produces valid JSON objects") {
    forAll { (node: TLiteralNode[String]) =>
      val json = node.asJson
      val obj = json.asObject.get
      obj("properties").flatMap(_.asObject).map(_.toMap) shouldBe Some(node.properties)

      val edges = obj("edges").flatMap(_.asArray).get
      edges.size shouldBe node.edges.size
      edges.zip(node.edges).foreach { case (edgeJson, edge) =>
        val edgeObj = edgeJson.asObject.get
        edgeObj("direction").flatMap(_.asString) shouldBe Some(edge.direction.toString)
        edgeObj("edgeType").flatMap(_.asString) shouldBe Some(edge.edgeType)
        edgeObj("other").flatMap(_.asString) shouldBe Some(edge.other)
      }
    }
  }
}

trait V2DebugEndpointCodecSpecGenerators extends ArbitraryCommon {
  import io.circe.Json
  import org.scalacheck.{Arbitrary, Gen}

  implicit val genTEdgeDirection: Gen[TEdgeDirection] = Gen.oneOf(TEdgeDirection.values)
  implicit val arbTEdgeDirection: Arbitrary[TEdgeDirection] = Arbitrary(genTEdgeDirection)

  val genJsonValue: Gen[Json] = Gen.frequency(
    (5, genJsonPrimitive),
    (1, Gen.listOfN(3, genJsonPrimitive).map(Json.fromValues)),
  )

  implicit def genTRestHalfEdge[ID: Arbitrary]: Gen[TRestHalfEdge[ID]] = for {
    edgeType <- Gen.alphaStr.suchThat(_.nonEmpty)
    direction <- genTEdgeDirection
    other <- Arbitrary.arbitrary[ID]
  } yield TRestHalfEdge(edgeType, direction, other)
  implicit def arbTRestHalfEdge[ID: Arbitrary]: Arbitrary[TRestHalfEdge[ID]] = Arbitrary(genTRestHalfEdge[ID])

  implicit def genTLiteralNode[ID: Arbitrary]: Gen[TLiteralNode[ID]] = for {
    propertiesSize <- Gen.chooseNum(0, 5)
    properties <- Gen.mapOfN(
      propertiesSize,
      Gen.zip(Gen.alphaStr.suchThat(_.nonEmpty), genJsonValue),
    )
    edgesSize <- Gen.chooseNum(0, 5)
    edges <- Gen.listOfN(edgesSize, genTRestHalfEdge[ID])
  } yield TLiteralNode(properties, edges)
  implicit def arbTLiteralNode[ID: Arbitrary]: Arbitrary[TLiteralNode[ID]] = Arbitrary(genTLiteralNode[ID])
}
