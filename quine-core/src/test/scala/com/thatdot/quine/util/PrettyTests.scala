package com.thatdot.quine.util

import java.util.UUID

import scala.collection.immutable.ArraySeq

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.thatdot.quine.graph.MultipleValuesStandingQueryPartId
import com.thatdot.quine.graph.cypher.{Expr, MultipleValuesStandingQuery}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, QuineId}

class PrettyTests extends AnyFlatSpec with ScalaCheckDrivenPropertyChecks {
  "tabString" should "generate empty string for negative/zero input" in {
    assert(Pretty.tabString(-1) == "")
  }

  "treePrint(UnitSq)" should "generate expected pretty output" in {
    val result = Pretty.treePrint(MultipleValuesStandingQuery.UnitSq.instance)

    val expected = "Unit"

    assert(result == expected)
  }

  "treePrint(exampleCross)" should "generate expected pretty output" in {
    val exampleCross = MultipleValuesStandingQuery.Cross(
      queries = ArraySeq(
        List(
          MultipleValuesStandingQuery.UnitSq.instance,
          MultipleValuesStandingQuery.UnitSq.instance
        ): _*
      ),
      emitSubscriptionsLazily = true
    )

    val result = Pretty.treePrint(exampleCross)

    val expected =
      s"""Cross (
         |\tqueries = List(
         |\t\tUnit,
         |\t\tUnit
         |\t),
         |\temitSubscriptionsLazily = true
         |)""".stripMargin

    assert(result == expected)
  }

  "treePrint(exampleLocalProp)" should "generate expected pretty output" in {
    val name = Symbol("name")
    val pname = Some(Symbol("p.name"))

    val exampleLocalProp = MultipleValuesStandingQuery.LocalProperty(
      propKey = name,
      propConstraint = MultipleValuesStandingQuery.LocalProperty.Any,
      aliasedAs = pname
    )

    val result = Pretty.treePrint(exampleLocalProp)

    val expected =
      s"""LocalProperty (
         |\tpropKey = $name,
         |\tpropConstraint = Any,
         |\taliasedAs = $pname
         |)""".stripMargin

    assert(result == expected)
  }

  "treePrint(exampleLocalId)" should "generate expected pretty output" in {
    val unknown = Symbol("unknown")

    val exampleLocalId = MultipleValuesStandingQuery.LocalId(
      aliasedAs = unknown,
      formatAsString = true
    )

    val result = Pretty.treePrint(exampleLocalId)

    val expected =
      s"""LocalId (
         |\taliasedAs = $unknown,
         |\tformatAsString = true
         |)""".stripMargin

    assert(result == expected)
  }

  "treePrint(exampleEdgeSub)" should "generate expected pretty output" in {
    val parent = Some(Symbol("Parent"))

    val exampleEdgeSub = MultipleValuesStandingQuery.SubscribeAcrossEdge(
      edgeName = parent,
      edgeDirection = Some(EdgeDirection.Outgoing),
      andThen = MultipleValuesStandingQuery.UnitSq.instance
    )

    val result = Pretty.treePrint(exampleEdgeSub)

    val expected =
      s"""SubscribeAcrossEdge (
        |\tedgeName = $parent,
        |\tedgeDirection = Some(Outgoing),
        |\tandThen =
        |\t\tUnit
        |)""".stripMargin

    assert(result == expected)
  }

  "treePrint(exampleEdgeRecip)" should "generate expected pretty output" in {
    val id = UUID.randomUUID()

    val unknown = Symbol("Unknown")

    val exampleEdgeRecip = MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(
      halfEdge = HalfEdge(unknown, EdgeDirection.Incoming, QuineId("hello, world".getBytes("ASCII"))),
      andThenId = MultipleValuesStandingQueryPartId(id)
    )

    val result = Pretty.treePrint(exampleEdgeRecip)

    val expected =
      s"""EdgeSubscriptionReciprocal (
         |\thalfEdge = HalfEdge($unknown,Incoming,QuineId(68656C6C6F2C20776F726C64)),
         |\tandThenId = MultipleValuesStandingQueryPartId($id)
         |)""".stripMargin

    assert(result == expected)
  }

  "treePrint(exampleFilterMap)" should "generated expected pretty output" in {
    val a = Symbol("a")

    val exampleFilterMap = MultipleValuesStandingQuery.FilterMap(
      condition = Some(Expr.Equal(Expr.Variable(a), Expr.Str("hello"))),
      toFilter = MultipleValuesStandingQuery.UnitSq.instance,
      dropExisting = true,
      toAdd = Nil
    )

    val result = Pretty.treePrint(exampleFilterMap)

    val expected =
      s"""FilterMap (
         |\tcondition = Some(Equal(Variable($a),Str(hello))),
         |\ttoFilter =
         |\t\tUnit,
         |\tdropExisting = true,
         |\ttoAdd = List()
         |)""".stripMargin

    assert(result == expected)
  }
}
