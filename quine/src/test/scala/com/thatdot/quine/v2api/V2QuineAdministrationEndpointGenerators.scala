package com.thatdot.quine.v2api

import java.time.Instant

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.quine.ScalaPrimitiveGenerators
import com.thatdot.quine.app.v2api.endpoints.V2AdministrationEndpointEntities._

object V2QuineAdministrationEndpointGenerators {

  import ScalaPrimitiveGenerators.Gens.{nonEmptyAlphaNumStr, optNonEmptyAlphaNumStr}

  object Gens {
    val tGraphHashCode: Gen[TGraphHashCode] = for {
      value <- nonEmptyAlphaNumStr
      atTime <- Gen.posNum[Long]
    } yield TGraphHashCode(value, atTime)

    val tQuineInfo: Gen[TQuineInfo] = for {
      version <- nonEmptyAlphaNumStr
      gitCommit <- optNonEmptyAlphaNumStr
      gitCommitDate <- optNonEmptyAlphaNumStr
      javaVersion <- nonEmptyAlphaNumStr
      javaRuntimeVersion <- nonEmptyAlphaNumStr
      javaAvailableProcessors <- Gen.posNum[Int]
      javaMaxMemory <- Gen.posNum[Long]
      persistenceWriteVersion <- nonEmptyAlphaNumStr
      quineType <- nonEmptyAlphaNumStr
    } yield TQuineInfo(
      version,
      gitCommit,
      gitCommitDate,
      javaVersion,
      javaRuntimeVersion,
      javaAvailableProcessors,
      javaMaxMemory,
      persistenceWriteVersion,
      quineType,
    )

    val tCounter: Gen[TCounter] = for {
      name <- nonEmptyAlphaNumStr
      count <- Gen.posNum[Long]
    } yield TCounter(name, count)

    val tNumericGauge: Gen[TNumericGauge] = for {
      name <- nonEmptyAlphaNumStr
      value <- Gen.posNum[Double]
    } yield TNumericGauge(name, value)

    val tTimerSummary: Gen[TTimerSummary] = for {
      name <- nonEmptyAlphaNumStr
      min <- Gen.posNum[Double]
      max <- Gen.posNum[Double]
      median <- Gen.posNum[Double]
      mean <- Gen.posNum[Double]
      q1 <- Gen.posNum[Double]
      q3 <- Gen.posNum[Double]
      oneMinuteRate <- Gen.posNum[Double]
      p90 <- Gen.posNum[Double]
      p99 <- Gen.posNum[Double]
      p80 <- Gen.posNum[Double]
      p20 <- Gen.posNum[Double]
      p10 <- Gen.posNum[Double]
    } yield TTimerSummary(name, min, max, median, mean, q1, q3, oneMinuteRate, p90, p99, p80, p20, p10)

    val tMetricsReport: Gen[TMetricsReport] = for {
      atTime <- Gen.posNum[Long].map(Instant.ofEpochMilli)
      counters <- Gen.listOfN(3, tCounter)
      timers <- Gen.listOfN(2, tTimerSummary)
      gauges <- Gen.listOfN(2, tNumericGauge)
    } yield TMetricsReport(atTime, counters, timers, gauges)

    val tShardInMemoryLimit: Gen[TShardInMemoryLimit] = for {
      softLimit <- Gen.posNum[Int]
      hardLimit <- Gen.posNum[Int]
    } yield TShardInMemoryLimit(softLimit, hardLimit)
  }

  object Arbs {
    implicit val tGraphHashCode: Arbitrary[TGraphHashCode] = Arbitrary(Gens.tGraphHashCode)
    implicit val tQuineInfo: Arbitrary[TQuineInfo] = Arbitrary(Gens.tQuineInfo)
    implicit val tCounter: Arbitrary[TCounter] = Arbitrary(Gens.tCounter)
    implicit val tNumericGauge: Arbitrary[TNumericGauge] = Arbitrary(Gens.tNumericGauge)
    implicit val tTimerSummary: Arbitrary[TTimerSummary] = Arbitrary(Gens.tTimerSummary)
    implicit val tMetricsReport: Arbitrary[TMetricsReport] = Arbitrary(Gens.tMetricsReport)
    implicit val tShardInMemoryLimit: Arbitrary[TShardInMemoryLimit] = Arbitrary(Gens.tShardInMemoryLimit)
  }
}
