package com.thatdot.data

import io.circe.Json
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class DataFoldableFromSpec extends AnyFunSpec with Matchers {
  describe("Chained foldables of the same type work") {
    it("works even if types are repeated") {
      val jsonValue = DataFoldableFrom.stringDataFoldable.fold("ABC", DataFolderTo.jsonFolder)
      jsonValue shouldBe Json.fromString("ABC")
      val jsonValue2 = DataFoldableFrom.jsonDataFoldable.fold(jsonValue, DataFolderTo.jsonFolder)
      jsonValue2 shouldEqual jsonValue
    }
  }
}
