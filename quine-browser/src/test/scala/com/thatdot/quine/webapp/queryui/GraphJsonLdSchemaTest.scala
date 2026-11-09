package com.thatdot.quine.webapp.queryui

import io.circe.Json
import io.circe.parser.parse
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.routes.{UiEdge, UiNode}

class GraphJsonLdSchemaTest extends AnyFunSuite with Matchers {

  private def encode(nodes: Seq[UiNode[String]], edges: Seq[UiEdge[String]]): Json =
    parse(GraphJsonLdSchema.encodeAsJsonLd(nodes, edges)).toOption.get

  private def graphNode(doc: Json, id: String): Json =
    doc.hcursor
      .downField("@graph")
      .values
      .get
      .find(_.hcursor.get[String]("@id").contains(GraphJsonLdSchema.nodeIri(id)))
      .getOrElse(fail(s"no node object for $id"))

  private def node(id: String, label: String = "", props: Map[String, Json] = Map.empty) =
    UiNode(id, 0, label, props)

  test("context declares JSON-LD 1.1 and the Quine vocabulary") {
    val ctx = encode(Seq.empty, Seq.empty).hcursor.downField("@context")
    ctx.get[Double]("@version") shouldBe Right(1.1)
    ctx.get[String]("@vocab") shouldBe Right(GraphJsonLdSchema.Vocab)
    ctx.get[String]("rdfs") shouldBe Right(GraphJsonLdSchema.RdfsNamespace)
  }

  test("node IDs become percent-encoded URNs") {
    val n = graphNode(encode(Seq(node("a b/c:d")), Seq.empty), "a b/c:d")
    n.hcursor.get[String]("@id") shouldBe Right("urn:quine:node:a%20b%2Fc%3Ad")
  }

  test("the display label becomes rdfs:label verbatim and never @type") {
    val n = graphNode(encode(Seq(node("a", label = "Employee:Person")), Seq.empty), "a")
    n.hcursor.get[String]("rdfs:label") shouldBe Right("Employee:Person")
    n.hcursor.downField("@type").succeeded shouldBe false
  }

  test("empty labels produce no rdfs:label") {
    graphNode(encode(Seq(node("a")), Seq.empty), "a").hcursor.downField("rdfs:label").succeeded shouldBe false
  }

  test("a user property named rdfs:label doesn't collide with the display label") {
    val n = graphNode(encode(Seq(node("a", "shown", Map("rdfs:label" -> Json.fromString("prop")))), Seq.empty), "a")
    n.hcursor.get[String]("rdfs:label") shouldBe Right("shown")
    n.hcursor.get[String]("rdfs%3Alabel") shouldBe Right("prop")
  }

  test("keyword-like and colon-containing keys are percent-encoded") {
    val n =
      graphNode(encode(Seq(node("a", props = Map("@id" -> Json.fromInt(1), "x:y" -> Json.fromInt(2)))), Seq.empty), "a")
    n.hcursor.get[Int]("%40id") shouldBe Right(1)
    n.hcursor.get[Int]("x%3Ay") shouldBe Right(2)
  }

  test("property values: scalars plain, scalar arrays as @list, everything else as @json") {
    val obj = Json.obj("k" -> Json.fromInt(1))
    val n = graphNode(
      encode(
        Seq(
          node(
            "a",
            props = Map(
              "s" -> Json.fromString("x"),
              "l" -> Json.arr(Json.fromInt(1), Json.fromInt(2)),
              "o" -> obj,
              "z" -> Json.Null,
            ),
          ),
        ),
        Seq.empty,
      ),
      "a",
    )
    n.hcursor.get[String]("s") shouldBe Right("x")
    n.hcursor.downField("l").get[Seq[Int]]("@list") shouldBe Right(Seq(1, 2))
    n.hcursor.downField("o").get[Json]("@value") shouldBe Right(obj)
    n.hcursor.downField("o").get[String]("@type") shouldBe Right("@json")
    n.hcursor.downField("z").get[Json]("@value") shouldBe Right(Json.Null)
  }

  test("directed edges are emitted from the source; undirected edges in both directions") {
    val doc = encode(
      Seq(node("a"), node("b")),
      Seq(UiEdge("a", "KNOWS", "b"), UiEdge("a", "NEAR", "b", isDirected = false)),
    )
    val ref = (id: String) => Json.obj("@id" -> Json.fromString(GraphJsonLdSchema.nodeIri(id)))
    graphNode(doc, "a").hcursor.get[Json]("KNOWS") shouldBe Right(ref("b"))
    graphNode(doc, "b").hcursor.downField("KNOWS").succeeded shouldBe false
    graphNode(doc, "a").hcursor.get[Json]("NEAR") shouldBe Right(ref("b"))
    graphNode(doc, "b").hcursor.get[Json]("NEAR") shouldBe Right(ref("a"))
  }

  test("a property and an edge type with the same name share one predicate") {
    val doc = encode(Seq(node("a", props = Map("rel" -> Json.fromInt(1))), node("b")), Seq(UiEdge("a", "rel", "b")))
    graphNode(doc, "a").hcursor.get[Seq[Json]]("rel").map(_.size) shouldBe Right(2)
  }

  test("edges from nodes outside the node set still get a subject") {
    val doc = encode(Seq(node("b")), Seq(UiEdge("a", "KNOWS", "b")))
    graphNode(doc, "a").hcursor.downField("KNOWS").succeeded shouldBe true
  }
}
