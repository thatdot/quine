package com.thatdot.quine.ingest2.core

import io.circe.Json
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.ingest2.core.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.{Expr => ce}

class DataFoldableFromSpec extends AnyFunSpec with Matchers {

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
  }

  describe("Chained foldables of the same type work") {
    it("works even if types are repeated") {
      val jsonValue = DataFoldableFrom.stringDataFoldable.fold("ABC", DataFolderTo.jsonFolder)
      jsonValue shouldBe Json.fromString("ABC")
      val jsonValue2 = DataFoldableFrom.jsonDataFoldable.fold(jsonValue, DataFolderTo.jsonFolder)
      jsonValue2 shouldEqual jsonValue
    }
  }

  //for protobuf dynamic message foldable test see [[ProtobufTest]]
}
