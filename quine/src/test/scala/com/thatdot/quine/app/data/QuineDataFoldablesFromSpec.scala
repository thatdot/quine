package com.thatdot.quine.app.data

import io.circe.Json
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

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

  //for protobuf dynamic message foldable test see [[ProtobufTest]]
}
