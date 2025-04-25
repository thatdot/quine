package com.thatdot.quine.ingest2.core

import io.circe.Json
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.app.model.ingest2.core.DataFoldableFrom._
import com.thatdot.quine.app.model.ingest2.core.DataFolderTo._
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.ingest2.core.FoldableTestData.toAnyDataFolder

class DataFolderToSpec extends AnyFunSpec with Matchers {

  describe("Folding to json") {
    val testData = FoldableTestData(mapValue = FoldableTestData().asMap, vectorValue = FoldableTestData().asVector)
    val testDataAsCypher = testData.foldTo[Value]
    val testDataAsJson = testData.foldTo[Json]

    it("converting to json yields uniform results") {
      val cypherBackToJson: Json = cypherValueDataFoldable.fold(testDataAsCypher, jsonFolder)
      testDataAsJson shouldBe cypherBackToJson
    }

    it("Cypher retains all type values") {
      val cypherToMap: Any = cypherValueDataFoldable.fold(testDataAsCypher, toAnyDataFolder)
      cypherToMap shouldBe testData.asMap
    }
    /*
    When we convert from csv, we can only read keys/values as strings, so the only meaningful
      comparison is from testData with values stringified.
     */
    it("Converts string map rows") {
      val testDataStringified: Map[String, String] = testData.asMap.view.mapValues(t => s"$t").to(Map)
      val v = stringMapDataFoldable.fold(testDataStringified, toAnyDataFolder)
      v shouldBe testDataStringified
    }

    it("Converts string vectors") {
      val testDataStringified = testData.asVector.map(t => s"$t")
      val v = stringIterableDataFoldable.fold(testDataStringified, toAnyDataFolder)
      v shouldBe testDataStringified
    }

  }
}
