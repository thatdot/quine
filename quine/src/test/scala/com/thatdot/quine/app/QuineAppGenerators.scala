package com.thatdot.quine.app

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.quine.ScalaPrimitiveGenerators
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultWorkflow
import com.thatdot.quine.graph.StandingQueryId
import com.thatdot.quine.outputs.StandingQueryOutputGenerators

object QuineAppGenerators {

  import ScalaPrimitiveGenerators.Gens.{nonEmptyAlphaNumStr, smallPosNum}
  import StandingQueryOutputGenerators.Gens.standingQueryResultWorkflow

  // Matches QuineApp.V2StandingQueryDataMap (private)
  type V2StandingQueryDataMap = Map[String, (StandingQueryId, Map[String, StandingQueryResultWorkflow])]

  object Gens {
    val standingQueryId: Gen[StandingQueryId] = Gen.uuid.map(StandingQueryId(_))

    val v2StandingQueryDataMap: Gen[V2StandingQueryDataMap] = for {
      numEntries <- smallPosNum
      entries <- Gen.listOfN(
        numEntries,
        for {
          sqName <- nonEmptyAlphaNumStr
          sqId <- standingQueryId
          numOutputs <- smallPosNum
          outputs <- Gen.listOfN(
            numOutputs,
            for {
              outputName <- nonEmptyAlphaNumStr
              workflow <- standingQueryResultWorkflow
            } yield outputName -> workflow,
          )
        } yield sqName -> (sqId, outputs.toMap),
      )
    } yield entries.toMap
  }

  object Arbs {
    implicit val standingQueryId: Arbitrary[StandingQueryId] = Arbitrary(Gens.standingQueryId)
    implicit val v2StandingQueryDataMap: Arbitrary[V2StandingQueryDataMap] = Arbitrary(Gens.v2StandingQueryDataMap)
  }
}
