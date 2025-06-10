package com.thatdot.quine.app.data

import io.circe.Json
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.data.DataFolderTo._
import com.thatdot.data.FoldableTestData
import com.thatdot.quine.app.data.QuineDataFoldablesFrom.cypherValueDataFoldable
import com.thatdot.quine.app.data.QuineDataFoldersTo.cypherValueFolder
import com.thatdot.quine.graph.cypher

class QuineDataFoldersToSpec extends AnyFunSpec with Matchers {

  describe("Folding to json") {
    val testData = FoldableTestData(mapValue = FoldableTestData().asMap, vectorValue = FoldableTestData().asVector)
    val testDataAsCypher = testData.foldTo[cypher.Value]
    val testDataAsJson = testData.foldTo[Json]

    it("converting to json yields uniform results") {
      val cypherBackToJson: Json = cypherValueDataFoldable.fold(testDataAsCypher, jsonFolder)
      testDataAsJson shouldBe cypherBackToJson
    }

    it("Cypher retains all type values") {
      val cypherToMap: Any = cypherValueDataFoldable.fold(testDataAsCypher, anyFolder)
      cypherToMap shouldBe testData.asMap
    }
  }
}
