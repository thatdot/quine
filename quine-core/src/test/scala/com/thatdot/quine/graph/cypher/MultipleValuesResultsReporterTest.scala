package com.thatdot.quine.graph.cypher

import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should

import com.thatdot.quine.graph.StandingQueryResult

class MultipleValuesResultsReporterTest extends AnyFunSpecLike with should.Matchers {
  val queryContext1: QueryContext = QueryContext(Map(Symbol("foo") -> Expr.Integer(1L)))
  val queryContext2: QueryContext = QueryContext(Map(Symbol("bar") -> Expr.Integer(2L)))
  val queryContext3: QueryContext = QueryContext(Map(Symbol("baz") -> Expr.Integer(3L)))

  def queryContextToResult(isPositive: Boolean, queryContext: QueryContext): StandingQueryResult =
    StandingQueryResult(isPositive, queryContext.environment.map(kv => kv._1.name -> Expr.toQuineValue(kv._2)))

  describe("MultipleValuesResultsReporter.generateResultReports") {
    it("includes all non-duplicate reports") {
      val oldResults = Seq(queryContext1)
      val newResults = Seq(queryContext2)

      val reportDiff =
        MultipleValuesResultsReporter.generateResultReports(oldResults, newResults, includeCancellations = true).toSeq

      reportDiff should contain theSameElementsAs Seq(
        queryContextToResult(isPositive = true, queryContext2),
        queryContextToResult(isPositive = false, queryContext1),
      )
    }
    it("omits duplicate reports") {
      val oldResults = Seq(queryContext3)
      val newResults = Seq(queryContext3)

      val reportDiff =
        MultipleValuesResultsReporter.generateResultReports(oldResults, newResults, includeCancellations = true).toSeq

      reportDiff should be(empty)
    }
    it("respects includeCancellations=false") {
      val oldResults = Seq(queryContext3)
      val newResults = Seq(queryContext1, queryContext2)

      val reportDiff =
        MultipleValuesResultsReporter.generateResultReports(oldResults, newResults, includeCancellations = false).toSeq

      reportDiff shouldNot contain(queryContextToResult(isPositive = false, queryContext3))
      reportDiff should contain theSameElementsAs Seq(
        queryContextToResult(isPositive = true, queryContext1),
        queryContextToResult(isPositive = true, queryContext2),
      )
    }
  }
}
