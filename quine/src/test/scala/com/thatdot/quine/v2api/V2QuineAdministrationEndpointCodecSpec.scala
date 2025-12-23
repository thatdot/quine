package com.thatdot.quine.v2api

import io.circe.syntax._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.app.v2api.endpoints.V2AdministrationEndpointEntities._

class V2QuineAdministrationEndpointCodecSpec extends AnyFunSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  import V2QuineAdministrationEndpointGenerators.Arbs._

  describe("TGraphHashCode encoder") {
    it("should encode with correct field names and values") {
      forAll { (hashCode: TGraphHashCode) =>
        val json = hashCode.asJson
        json.hcursor.downField("value").as[String] shouldBe Right(hashCode.value)
        json.hcursor.downField("atTime").as[Long] shouldBe Right(hashCode.atTime)
      }
    }
  }

  describe("TQuineInfo encoder") {
    it("should encode with correct field names and values") {
      forAll { (info: TQuineInfo) =>
        val json = info.asJson
        json.hcursor.downField("version").as[String] shouldBe Right(info.version)
        json.hcursor.downField("gitCommit").as[Option[String]] shouldBe Right(info.gitCommit)
        json.hcursor.downField("javaVersion").as[String] shouldBe Right(info.javaVersion)
        json.hcursor.downField("javaAvailableProcessors").as[Int] shouldBe Right(info.javaAvailableProcessors)
        json.hcursor.downField("quineType").as[String] shouldBe Right(info.quineType)
      }
    }
  }

  describe("TCounter encoder") {
    it("should encode with correct field names and values") {
      forAll { (counter: TCounter) =>
        val json = counter.asJson
        json.hcursor.downField("name").as[String] shouldBe Right(counter.name)
        json.hcursor.downField("count").as[Long] shouldBe Right(counter.count)
      }
    }
  }

  describe("TNumericGauge encoder") {
    it("should encode with correct field names and values") {
      forAll { (gauge: TNumericGauge) =>
        val json = gauge.asJson
        json.hcursor.downField("name").as[String] shouldBe Right(gauge.name)
        json.hcursor.downField("value").as[Double] shouldBe Right(gauge.value)
      }
    }
  }

  describe("TTimerSummary encoder") {
    it("should encode with correct field names and values") {
      forAll { (timer: TTimerSummary) =>
        val json = timer.asJson
        json.hcursor.downField("name").as[String] shouldBe Right(timer.name)
        json.hcursor.downField("min").as[Double] shouldBe Right(timer.min)
        json.hcursor.downField("max").as[Double] shouldBe Right(timer.max)
        json.hcursor.downField("median").as[Double] shouldBe Right(timer.median)
        json.hcursor.downField("mean").as[Double] shouldBe Right(timer.mean)
        json.hcursor.downField("90").as[Double] shouldBe Right(timer.`90`)
        json.hcursor.downField("99").as[Double] shouldBe Right(timer.`99`)
      }
    }
  }

  describe("TMetricsReport encoder") {
    it("should encode with correct field values") {
      forAll { (report: TMetricsReport) =>
        val json = report.asJson
        json.hcursor.downField("atTime").as[String] shouldBe Right(report.atTime.toString)
        json.hcursor.downField("counters").as[List[io.circe.Json]].map(_.size) shouldBe Right(report.counters.size)
        json.hcursor.downField("timers").as[List[io.circe.Json]].map(_.size) shouldBe Right(report.timers.size)
        json.hcursor.downField("gauges").as[List[io.circe.Json]].map(_.size) shouldBe Right(report.gauges.size)
      }
    }

    it("should encode nested counter values correctly") {
      forAll { (report: TMetricsReport) =>
        whenever(report.counters.nonEmpty) {
          val json = report.asJson
          val firstCounter = json.hcursor.downField("counters").downArray
          firstCounter.downField("name").as[String] shouldBe Right(report.counters.head.name)
          firstCounter.downField("count").as[Long] shouldBe Right(report.counters.head.count)
        }
      }
    }
  }

  describe("TShardInMemoryLimit codec") {
    it("should roundtrip encode/decode") {
      forAll { (limit: TShardInMemoryLimit) =>
        val json = limit.asJson
        val decoded = json.as[TShardInMemoryLimit]
        decoded shouldBe Right(limit)
      }
    }

    it("should encode with correct field names and values") {
      forAll { (limit: TShardInMemoryLimit) =>
        val json = limit.asJson
        json.hcursor.downField("softLimit").as[Int] shouldBe Right(limit.softLimit)
        json.hcursor.downField("hardLimit").as[Int] shouldBe Right(limit.hardLimit)
      }
    }
  }
}
