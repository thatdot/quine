package com.thatdot.quine.app.data

import io.circe.Json
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.quineid.QuineId
import com.thatdot.data.DataFoldableFrom._
import com.thatdot.data.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.app.data.QuineDataFoldablesFrom.cypherValueDataFoldable
import com.thatdot.quine.app.data.QuineDataFoldersTo.cypherValueFolder
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.{Expr => ce}

class QuineDataFoldablesFromSpec extends AnyFunSpec with Matchers {

  describe("DataFoldable[Json]") {
    it("properly round trips to cypher") {

      val original = Json.obj(
        "foo" -> Json.fromString("bar"),
        "baz" -> Json.fromLong(7),
        "qux" -> Json.arr(
          Json.fromBoolean(true),
          Json.obj(
            "zip" -> Json.Null,
          ),
        ),
      )
      val result = DataFoldableFrom[Json].fold[cypher.Value](original, DataFolderTo[cypher.Value])
      val expected = ce.Map(
        "foo" -> ce.Str("bar"),
        "baz" -> ce.Integer(7),
        "qux" -> ce.List(
          ce.True,
          ce.Map(
            "zip" -> ce.Null,
          ),
        ),
      )
      result shouldBe expected
    }
  }

  describe("DataFoldable[cypher.Value]") {
    it("round trips a supported Cypher value") {
      val original = ce.Map(
        "foo" -> ce.Str("bar"),
        "baz" -> ce.Integer(7),
        "qux" -> ce.List(
          ce.True,
          ce.Map(
            "zip" -> ce.Null,
          ),
        ),
      )

      val result = DataFoldableFrom[cypher.Value].fold[cypher.Value](original, DataFolderTo[cypher.Value])
      result shouldBe original
    }

    describe("Expr.Bytes representsId hint") {
      val raw = Array[Byte](0x00, 0xFF.toByte, 0x80.toByte, 0x7F)

      it("plain bytes (representsId=false) dispatch to folder.bytes — JSON emits base64") {
        val plain: cypher.Value = ce.Bytes(raw, representsId = false)
        DataFoldableFrom[cypher.Value].fold[Json](plain, DataFolderTo[Json]) shouldEqual
        Json.fromString("AP+Afw==")
      }

      it("id bytes (representsId=true) dispatch to folder.id — JSON emits canonical hex") {
        val id: cypher.Value = ce.Bytes(raw, representsId = true)
        DataFoldableFrom[cypher.Value].fold[Json](id, DataFolderTo[Json]) shouldEqual
        Json.fromString("00FF807F")
      }

      it("id bytes round-trip through the Cypher folder preserve the representsId flag") {
        val id: cypher.Value = ce.Bytes(raw, representsId = true)
        val out = DataFoldableFrom[cypher.Value].fold[cypher.Value](id, cypherValueFolder)
        out shouldEqual ce.Bytes(raw, representsId = true)
        // Bytes.equals ignores the flag, so check it explicitly.
        out.asInstanceOf[ce.Bytes].representsId shouldEqual true
      }
    }
  }

  describe("graph values fold to the text `/cypher` API's JSON shape") {
    it("a Node folds to {id, labels, properties} with a hex-encoded id") {
      val node: cypher.Value = ce.Node(
        QuineId(Array[Byte](0x00, 0xFF.toByte, 0x80.toByte, 0x7F)),
        Set(Symbol("Person")),
        Map(Symbol("name") -> ce.Str("Alice"), Symbol("age") -> ce.Integer(30)),
      )
      DataFoldableFrom[cypher.Value].fold[Json](node, DataFolderTo[Json]) shouldEqual Json.obj(
        "id" -> Json.fromString("00FF807F"),
        "labels" -> Json.arr(Json.fromString("Person")),
        "properties" -> Json.obj("name" -> Json.fromString("Alice"), "age" -> Json.fromLong(30)),
      )
    }

    it("a Relationship folds to {start, end, name, properties}") {
      val rel: cypher.Value = ce.Relationship(
        QuineId(Array[Byte](0x01)),
        Symbol("KNOWS"),
        Map(Symbol("since") -> ce.Integer(2020)),
        QuineId(Array[Byte](0x02)),
      )
      DataFoldableFrom[cypher.Value].fold[Json](rel, DataFolderTo[Json]) shouldEqual Json.obj(
        "start" -> Json.fromString("01"),
        "end" -> Json.fromString("02"),
        "name" -> Json.fromString("KNOWS"),
        "properties" -> Json.obj("since" -> Json.fromLong(2020)),
      )
    }

    it("a Path folds to the flattened alternating list of its node/relationship elements") {
      val a = ce.Node(QuineId(Array[Byte](0x0A)), Set(Symbol("A")), Map.empty)
      val b = ce.Node(QuineId(Array[Byte](0x0B)), Set(Symbol("B")), Map.empty)
      val r = ce.Relationship(QuineId(Array[Byte](0x0A)), Symbol("R"), Map.empty, QuineId(Array[Byte](0x0B)))
      val path: cypher.Value = ce.Path(a, Vector((r, b)))
      DataFoldableFrom[cypher.Value].fold[Json](path, DataFolderTo[Json]) shouldEqual Json.arr(
        Json.obj("id" -> Json.fromString("0A"), "labels" -> Json.arr(Json.fromString("A")), "properties" -> Json.obj()),
        Json.obj(
          "start" -> Json.fromString("0A"),
          "end" -> Json.fromString("0B"),
          "name" -> Json.fromString("R"),
          "properties" -> Json.obj(),
        ),
        Json.obj("id" -> Json.fromString("0B"), "labels" -> Json.arr(Json.fromString("B")), "properties" -> Json.obj()),
      )
    }
  }

  //for protobuf dynamic message foldable test see [[ProtobufTest]]
}
