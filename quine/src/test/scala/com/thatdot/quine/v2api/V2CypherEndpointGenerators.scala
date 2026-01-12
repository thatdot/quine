package com.thatdot.quine.v2api

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.v2api.endpoints.V2CypherEndpointEntities.{
  TCypherQuery,
  TCypherQueryResult,
  TUiEdge,
  TUiNode,
}
import com.thatdot.quine.graph.QuineIdLongProvider
import com.thatdot.quine.{JsonGenerators, ScalaPrimitiveGenerators}

object V2CypherEndpointGenerators {
  import ScalaPrimitiveGenerators.Gens.{bool, nonEmptyAlphaNumStr, nonEmptyAlphaStr, smallNonNegNum, smallPosNum}
  import JsonGenerators.Gens.{dictionary, primitive}

  private val longProvider: QuineIdLongProvider = QuineIdLongProvider()

  object Gens {
    val quineIdFromLong: Gen[QuineId] = Arbitrary.arbLong.arbitrary.map(longProvider.customIdToQid)

    val tCypherQuery: Gen[TCypherQuery] = for {
      text <- nonEmptyAlphaNumStr
      params <- dictionary
    } yield TCypherQuery(text, params)

    val tCypherQueryResult: Gen[TCypherQueryResult] = for {
      numCols <- smallPosNum
      columns <- Gen.listOfN(numCols, nonEmptyAlphaStr)
      numRows <- smallNonNegNum
      results <- Gen.listOfN(numRows, Gen.listOfN(numCols, primitive))
    } yield TCypherQueryResult(columns, results)

    val tUiNode: Gen[TUiNode] = for {
      id <- quineIdFromLong
      hostIndex <- smallNonNegNum
      label <- nonEmptyAlphaStr
      properties <- dictionary
    } yield TUiNode(id, hostIndex, label, properties)

    val tUiEdge: Gen[TUiEdge] = for {
      from <- quineIdFromLong
      edgeType <- nonEmptyAlphaStr
      to <- quineIdFromLong
      isDirected <- bool
    } yield TUiEdge(from, edgeType, to, isDirected)
  }

  object Arbs {
    implicit val quineId: Arbitrary[QuineId] = Arbitrary(Gens.quineIdFromLong)
    implicit val tCypherQuery: Arbitrary[TCypherQuery] = Arbitrary(Gens.tCypherQuery)
    implicit val tCypherQueryResult: Arbitrary[TCypherQueryResult] = Arbitrary(Gens.tCypherQueryResult)
    implicit val tUiNode: Arbitrary[TUiNode] = Arbitrary(Gens.tUiNode)
    implicit val tUiEdge: Arbitrary[TUiEdge] = Arbitrary(Gens.tUiEdge)
  }
}
