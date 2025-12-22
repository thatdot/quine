package com.thatdot.quine.v2api

import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.app.v2api.endpoints.V2DebugEndpointEntities.{TEdgeDirection, TLiteralNode, TRestHalfEdge}

class V2DebugEndpointCodecSpec extends AnyFunSuite with Matchers with ScalaCheckDrivenPropertyChecks {
  import V2DebugEndpointGenerators.Arbs._

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
