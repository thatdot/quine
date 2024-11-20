package com.thatdot.quine.util

import com.thatdot.quine.graph.cypher.MultipleValuesStandingQuery

object Pretty {

  def tabString(numTabs: Int): String =
    (1 to numTabs).map(_ => "\t").mkString

  def treePrint(mvsq: MultipleValuesStandingQuery, indent: Int): String = {
    val sp = tabString(indent)
    val sp1 = tabString(indent + 1)
    mvsq match {
      case _: MultipleValuesStandingQuery.UnitSq => sp + "Unit"
      case MultipleValuesStandingQuery.Cross(queries, emitSubscriptionsLazily, _) =>
        val queriesString = s"List(${queries.map(q => treePrint(q, indent + 2)).mkString("\n", ",\n", ",\n")}$sp1)"
        s"""${sp}Cross (
           |${sp1}queries = $queriesString,
           |${sp1}emitSubscriptionsLazily = $emitSubscriptionsLazily,
           |${sp})""".stripMargin
      case MultipleValuesStandingQuery.LocalProperty(propKey, propConstraint, aliasedAs, _) =>
        s"""${sp}LocalProperty (
           |${sp1}propKey = $propKey,
           |${sp1}propConstraint = $propConstraint,
           |${sp1}aliasedAs = $aliasedAs,
           |${sp})""".stripMargin
      case MultipleValuesStandingQuery.Labels(aliasedAs, constraint, _) =>
        s"""${sp}Labels (
           |${sp1}aliasedAs = $aliasedAs,
           |${sp1}constraint = $constraint,
           |${sp})""".stripMargin
      case MultipleValuesStandingQuery.AllProperties(aliasedAs, _) =>
        s"""${sp}AllProperties (
           |${sp1}aliasedAs = $aliasedAs,
           |${sp})""".stripMargin
      case MultipleValuesStandingQuery.LocalId(aliasedAs, formatAsString, _) =>
        s"""${sp}LocalId (
           |${sp1}aliasedAs = $aliasedAs,
           |${sp1}formatAsString = $formatAsString,
           |${sp})""".stripMargin
      case MultipleValuesStandingQuery.SubscribeAcrossEdge(edgeName, edgeDirection, andThen, _) =>
        s"""${sp}SubscribeAcrossEdge (
           |${sp1}edgeName = $edgeName,
           |${sp1}edgeDirection = $edgeDirection,
           |${sp1}andThen =
           |${treePrint(andThen, indent + 2)},
           |${sp})""".stripMargin
      case MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(halfEdge, andThenId, _) =>
        s"""${sp}EdgeSubscriptionReciprocal (
           |${sp1}halfEdge = $halfEdge,
           |${sp1}andThenId = $andThenId,
           |${sp})""".stripMargin
      case MultipleValuesStandingQuery.FilterMap(condition, toFilter, dropExisting, toAdd, _) =>
        s"""${sp}FilterMap (
           |${sp1}condition = $condition,
           |${sp1}toFilter =
           |${treePrint(toFilter, indent + 2)},
           |${sp1}dropExisting = $dropExisting,
           |${sp1}toAdd = $toAdd,
           |${sp})""".stripMargin
    }
  }

  def treePrint(mvsq: MultipleValuesStandingQuery): String = treePrint(mvsq, 0)
}
