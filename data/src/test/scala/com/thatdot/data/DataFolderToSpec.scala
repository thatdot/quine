package com.thatdot.data

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import com.thatdot.data.DataFoldableFrom._

class DataFolderToSpec extends AnyFunSpec with Matchers {

  private def nullSafeToString(x: Any) = s"$x"

  describe("DataFolderTo") {
    it("preserves map values across a fold") {
      val testDataStringified: Map[String, String] = FoldableTestData().asMap.view.mapValues(nullSafeToString).to(Map)
      val v = stringMapDataFoldable.fold(testDataStringified, DataFolderTo.anyFolder)
      v shouldBe testDataStringified
    }

    it("preserves vector values across a fold") {
      val testDataStringified = FoldableTestData().asVector.map(nullSafeToString)
      val v = stringIterableDataFoldable.fold(testDataStringified, DataFolderTo.anyFolder)
      v shouldBe testDataStringified
    }
  }

}
