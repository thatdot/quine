package com.thatdot.quine.v2api

import io.circe.Json
import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.quine.app.v2api.endpoints.V2DebugEndpointEntities.{TEdgeDirection, TLiteralNode, TRestHalfEdge}
import com.thatdot.quine.{JsonGenerators, ScalaPrimitiveGenerators}

object V2DebugEndpointGenerators {
  import ScalaPrimitiveGenerators.Gens.{nonEmptyAlphaStr, smallNum}
  import JsonGenerators.Gens.primitive

  object Gens {
    val tEdgeDirection: Gen[TEdgeDirection] = Gen.oneOf(TEdgeDirection.values)

    val jsonValue: Gen[Json] = Gen.frequency(
      (5, primitive),
      (1, Gen.listOfN(3, primitive).map(Json.fromValues)),
    )

    def tRestHalfEdge[ID: Arbitrary]: Gen[TRestHalfEdge[ID]] = for {
      edgeType <- nonEmptyAlphaStr
      direction <- tEdgeDirection
      other <- Arbitrary.arbitrary[ID]
    } yield TRestHalfEdge(edgeType, direction, other)

    def tLiteralNode[ID: Arbitrary]: Gen[TLiteralNode[ID]] = for {
      propertiesSize <- smallNum
      properties <- Gen.mapOfN(propertiesSize, Gen.zip(nonEmptyAlphaStr, jsonValue))
      edgesSize <- smallNum
      edges <- Gen.listOfN(edgesSize, tRestHalfEdge[ID])
    } yield TLiteralNode(properties, edges)
  }

  object Arbs {
    implicit val tEdgeDirection: Arbitrary[TEdgeDirection] = Arbitrary(Gens.tEdgeDirection)
    implicit def tRestHalfEdge[ID: Arbitrary]: Arbitrary[TRestHalfEdge[ID]] = Arbitrary(Gens.tRestHalfEdge[ID])
    implicit def tLiteralNode[ID: Arbitrary]: Arbitrary[TLiteralNode[ID]] = Arbitrary(Gens.tLiteralNode[ID])
  }
}
