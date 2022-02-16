package com.thatdot.quine.app

import java.util.regex.Pattern

import com.thatdot.quine.compiler.cypher
import com.thatdot.quine.graph.GraphQueryPattern.{
  EdgePattern => PlannerEdgePattern,
  NodePattern => PlannerNodePattern,
  NodePatternId => PlannerNodePatternId,
  PropertyValuePattern => PlannerPropertyValuePattern,
  ReturnColumn => PlannerReturnColumn
}
import com.thatdot.quine.graph.{BaseGraph, GraphQueryPattern}
import com.thatdot.quine.model.{QuineIdProvider, QuineValue}
import com.thatdot.quine.routes.{StandingQueryDefinition, StandingQueryPattern}

import StandingQueryPattern._

/** Conversion functions from API types to/from Quine types */
object ApiConverters {

  /** Convert a REST API node pattern into a Quine one */
  def runtimeNodePattern(nodePat: NodePattern): PlannerNodePattern =
    GraphQueryPattern.NodePattern(
      id = PlannerNodePatternId(nodePat.patternId),
      labels = nodePat.labels.map(Symbol.apply),
      qidOpt = None,
      properties = nodePat.properties.map(kvs => Symbol(kvs._1) -> runtimePropertyValuePattern(kvs._2)).toMap
    )

  /** Convert a Quine node pattern into a REST API one */
  def nodePattern(nodePat: PlannerNodePattern)(implicit idProvider: QuineIdProvider): NodePattern =
    NodePattern(
      patternId = nodePat.id.id,
      labels = nodePat.labels.map(_.name),
      properties = nodePat.properties.map(kvs => kvs._1.name -> propertyValuePattern(kvs._2)).toMap
    )

  /** Convert a REST API property value pattern into a Quine one */
  def runtimePropertyValuePattern(valuePat: PropertyValuePattern): PlannerPropertyValuePattern =
    valuePat match {
      case PropertyValuePattern.NoValue => PlannerPropertyValuePattern.NoValue
      case PropertyValuePattern.AnyValue => PlannerPropertyValuePattern.AnyValue
      case PropertyValuePattern.Value(j) => PlannerPropertyValuePattern.Value(QuineValue.fromJson(j))
      case PropertyValuePattern.AnyValueExcept(j) => PlannerPropertyValuePattern.AnyValueExcept(QuineValue.fromJson(j))
      case PropertyValuePattern.RegexMatch(pat) => PlannerPropertyValuePattern.RegexMatch(Pattern.compile(pat))
    }

  /** Convert a Quine property value pattern into a REST API one */
  def propertyValuePattern(
    valuePat: GraphQueryPattern.PropertyValuePattern
  )(implicit idProvider: QuineIdProvider): StandingQueryPattern.PropertyValuePattern =
    valuePat match {
      case PlannerPropertyValuePattern.NoValue => PropertyValuePattern.NoValue
      case PlannerPropertyValuePattern.AnyValue => PropertyValuePattern.AnyValue
      case PlannerPropertyValuePattern.Value(qv) => PropertyValuePattern.Value(QuineValue.toJson(qv))
      case PlannerPropertyValuePattern.AnyValueExcept(qv) => PropertyValuePattern.AnyValueExcept(QuineValue.toJson(qv))
      case PlannerPropertyValuePattern.RegexMatch(re) => PropertyValuePattern.RegexMatch(re.pattern)
    }

  /** Convert a REST API edge pattern into a Quine one */
  def runtimeEdgePattern(edgePat: EdgePattern): PlannerEdgePattern =
    PlannerEdgePattern(
      from = PlannerNodePatternId(edgePat.from),
      to = PlannerNodePatternId(edgePat.to),
      isDirected = edgePat.isDirected,
      label = Symbol(edgePat.label)
    )

  /** Convert a Quine edge pattern into a REST API one */
  def edgePattern(edgePat: PlannerEdgePattern): EdgePattern =
    EdgePattern(
      from = edgePat.from.id,
      to = edgePat.to.id,
      isDirected = edgePat.isDirected,
      label = edgePat.label.name
    )

  /** Convert a REST API return column into a Quine one */
  def runtimeReturnColumn(retCol: ReturnColumn): PlannerReturnColumn =
    retCol match {
      case ReturnColumn.Id(n, f, a) => PlannerReturnColumn.Id(PlannerNodePatternId(n), f, Symbol(a))
      case ReturnColumn.Property(n, k, a) => PlannerReturnColumn.Property(PlannerNodePatternId(n), Symbol(k), Symbol(a))
    }

  /** Convert a Quine return column into a REST API one */
  def returnColumn(retCol: PlannerReturnColumn): ReturnColumn =
    retCol match {
      case PlannerReturnColumn.Id(n, f, a) => ReturnColumn.Id(n.id, f, a.name)
      case PlannerReturnColumn.Property(n, k, a) => ReturnColumn.Property(n.id, k.name, a.name)
    }

  /** Convert a REST API standing query definition into a Quine one */
  def compileToInternalSqPattern(
    query: StandingQueryDefinition,
    graph: BaseGraph
  ): com.thatdot.quine.graph.StandingQueryPattern =
    query.pattern match {
      case StandingQueryPattern.Graph(nodes, edges, startingPoint, toExtract, mode) =>
        com.thatdot.quine.graph.StandingQueryPattern.fromGraphPattern(
          GraphQueryPattern(
            nodes.map(runtimeNodePattern),
            edges.map(runtimeEdgePattern),
            GraphQueryPattern.NodePatternId(startingPoint),
            toExtract.map(runtimeReturnColumn),
            None,
            Nil,
            mode == StandingQueryMode.DistinctId // QU-568 currently, the `DistinctId` mode is the only way to express the `distinct`ness property of a (non-Cypher) GraphPattern
          ),
          None,
          query.includeCancellations,
          mode == StandingQueryMode.DistinctId,
          graph.labelsProperty,
          graph.idProvider
        )

      case StandingQueryPattern.Cypher(cypherQuery, mode) =>
        com.thatdot.quine.graph.StandingQueryPattern.fromGraphPattern(
          cypher.compileStandingQueryGraphPattern(cypherQuery)(graph.idProvider),
          Some(cypherQuery),
          query.includeCancellations,
          mode == StandingQueryMode.DistinctId,
          graph.labelsProperty,
          graph.idProvider
        )
    }

}
