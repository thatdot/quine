package com.thatdot.quine.v2api

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.api.v2.RatesSummary
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQuery._
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern._
import com.thatdot.quine.app.v2api.definitions.query.standing.{StandingQueryPattern, StandingQueryStats}
import com.thatdot.quine.outputs.StandingQueryOutputGenerators
import com.thatdot.quine.{ScalaPrimitiveGenerators, TimeGenerators}

object V2StandingEndpointGenerators {

  import ScalaPrimitiveGenerators.Gens._
  import StandingQueryOutputGenerators.Gens._
  import TimeGenerators.Gens._

  object Gens {
    val ratesSummary: Gen[RatesSummary] = for {
      count <- Gen.posNum[Long]
      oneMinute <- Gen.posNum[Double]
      fiveMinute <- Gen.posNum[Double]
      fifteenMinute <- Gen.posNum[Double]
      overall <- Gen.posNum[Double]
    } yield RatesSummary(count, oneMinute, fiveMinute, fifteenMinute, overall)

    val standingQueryStats: Gen[StandingQueryStats] = for {
      rates <- ratesSummary
      startTime <- instant
      totalRuntime <- Gen.posNum[Long]
      bufferSize <- Gen.posNum[Int]
      outputHashCode <- Gen.posNum[Long]
    } yield StandingQueryStats(rates, startTime, totalRuntime, bufferSize, outputHashCode)

    val standingQueryMode: Gen[StandingQueryMode] = Gen.oneOf(
      StandingQueryMode.DistinctId,
      StandingQueryMode.MultipleValues,
      StandingQueryMode.QuinePattern,
    )

    val cypherPattern: Gen[Cypher] = for {
      query <- nonEmptyAlphaNumStr.map(s => s"MATCH (n) WHERE n.id = '$s' RETURN DISTINCT n")
      mode <- standingQueryMode
    } yield Cypher(query, mode)

    val standingQueryPattern: Gen[StandingQueryPattern] = cypherPattern

    val standingQueryDefinition: Gen[StandingQueryDefinition] = for {
      name <- nonEmptyAlphaNumStr
      pattern <- standingQueryPattern
      outputs <- Gen.listOfN(2, standingQueryResultWorkflow)
      includeCancellations <- bool
      inputBufferSize <- numWithinBits(7)
    } yield StandingQueryDefinition(name, pattern, outputs, includeCancellations, inputBufferSize)

    val registeredStandingQuery: Gen[RegisteredStandingQuery] = for {
      name <- nonEmptyAlphaNumStr
      internalId <- Gen.uuid
      pattern <- Gen.option(standingQueryPattern)
      outputs <- Gen.listOfN(2, standingQueryResultWorkflow)
      includeCancellations <- bool
      inputBufferSize <- numWithinBits(7)
      statsCount <- smallPosNum
      statsHosts <- Gen.listOfN(statsCount, nonEmptyAlphaNumStr)
      statsValues <- Gen.listOfN(statsCount, standingQueryStats)
      stats = statsHosts.zip(statsValues).toMap
    } yield RegisteredStandingQuery(name, internalId, pattern, outputs, includeCancellations, inputBufferSize, stats)
  }

  object Arbs {
    implicit val ratesSummary: Arbitrary[RatesSummary] = Arbitrary(Gens.ratesSummary)
    implicit val standingQueryStats: Arbitrary[StandingQueryStats] = Arbitrary(Gens.standingQueryStats)
    implicit val standingQueryMode: Arbitrary[StandingQueryMode] = Arbitrary(Gens.standingQueryMode)
    implicit val cypherPattern: Arbitrary[Cypher] = Arbitrary(Gens.cypherPattern)
    implicit val standingQueryPattern: Arbitrary[StandingQueryPattern] = Arbitrary(Gens.standingQueryPattern)
    implicit val standingQueryDefinition: Arbitrary[StandingQueryDefinition] = Arbitrary(Gens.standingQueryDefinition)
    implicit val registeredStandingQuery: Arbitrary[RegisteredStandingQuery] = Arbitrary(Gens.registeredStandingQuery)
  }
}
