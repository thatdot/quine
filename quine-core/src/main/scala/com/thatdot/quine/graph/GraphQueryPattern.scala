package com.thatdot.quine.graph

import java.util.regex.Pattern

import scala.collection.compat._
import scala.collection.compat.immutable._
import scala.collection.mutable

import com.thatdot.quine.model
import com.thatdot.quine.model.{EdgeDirection, QuineId, QuineIdProvider, QuineValue}

final case class InvalidQueryPattern(message: String) extends RuntimeException(message)

/** Representation of a graph query
  *
  * @param nodes node patterns in the query
  * @param edges edge patterns in the query
  * @param startingPoint which node should be the starting point of the query?
  * @param toExtract all the columns that are needed
  * @param filterCond expression using `toExtract`
  * @param toReturn columns to return (if empty, just return the columns as is)
  * @param distinct whether the returned values should be deduplicated
  *                 (see [[com.thatdot.quine.routes.StandingQueryPattern.StandingQueryMode.DistinctId]])
  */
final case class GraphQueryPattern(
  nodes: Seq[GraphQueryPattern.NodePattern],
  edges: Seq[GraphQueryPattern.EdgePattern],
  startingPoint: GraphQueryPattern.NodePatternId,
  toExtract: Seq[GraphQueryPattern.ReturnColumn],
  filterCond: Option[cypher.Expr],
  toReturn: Seq[(Symbol, cypher.Expr)],
  distinct: Boolean
) {
  assert(nodes.nonEmpty, "A query pattern needs a non-zero number of nodes")

  import GraphQueryPattern._

  /** Turn the declarative graph pattern into a query plan
    *
    * Normally, the hard part of this problem is finding an optimal plan. This
    * problem is hard because it involves searching a wide space of possible
    * query plans and applying heuristics based on some aggregate information
    * maintained about the data (things like: node count, label count,
    * estimated cardinality of edges, indices, etc.). Since we have none of that
    * information, this step is relatively easy: we just chose any plan.
    *
    * @note will fail if the graph pattern is not connected
    * @note will fail if the graph pattern has a loop of some sort
    * @note despite being ad-hoc, this is deterministic
    *
    * @return query that matches this graph pattern
    */
  @throws[InvalidQueryPattern]
  def compiledDomainGraphBranch(
    labelsProperty: Symbol
  ): (model.SingleBranch, ReturnColumn.Id) = {

    if (nodes.isEmpty) {
      throw InvalidQueryPattern("Pattern must be non-empty")
    } else if (filterCond.nonEmpty) {
      throw InvalidQueryPattern("DistinctId queries cannot filter")
    } else if (toReturn.nonEmpty) {
      throw InvalidQueryPattern("DistinctId queries cannot map")
    }

    val returnColumn = toExtract match {
      case Seq(returnCol @ ReturnColumn.Id(returnNodeId, _, _)) if returnNodeId == startingPoint => returnCol
      case _ => throw InvalidQueryPattern("DistinctId queries must return exactly the `id` of the root node")
    }

    // Keep track of which bits of the pattern are still unexplored
    val remainingNodes = mutable.Map.apply(nodes.map(pat => pat.id -> pat): _*)
    var remainingEdges = edges

    // Extract a DGB rooted at the given pattern
    def synthesizeBranch(id: NodePatternId): model.SingleBranch = {

      val NodePattern(_, labels, qidOpt, props) = remainingNodes.remove(id).getOrElse {
        throw InvalidQueryPattern("Pattern has a cycle")
      }

      val (connectedEdges, otherEdges) = remainingEdges.partition(e => e.from == id || e.to == id)
      remainingEdges = otherEdges

      val domainEdges = List.newBuilder[model.DomainEdge]
      val circularEdges = Set.newBuilder[model.CircularEdge]

      for (EdgePattern(from, to, isDirected, label) <- connectedEdges)
        if (from == id && to == id) {
          circularEdges += (label -> isDirected)
        } else if (from == id) {
          val edgeDir = if (isDirected) EdgeDirection.Outgoing else EdgeDirection.Undirected
          domainEdges += model.DomainEdge(
            edge = model.GenericEdge(label, edgeDir),
            depDirection = model.DependsUpon, // really anything will do
            branch = synthesizeBranch(to)
          )
        } else
          /* if (to == id) */ {
            val edgeDir = if (isDirected) EdgeDirection.Incoming else EdgeDirection.Undirected
            domainEdges += model.DomainEdge(
              edge = model.GenericEdge(label, edgeDir),
              depDirection = model.DependsUpon, // really anything will do
              branch = synthesizeBranch(from)
            )
          }

      val localProps = props.view.mapValues {
        case PropertyValuePattern.AnyValue =>
          model.PropertyComparisonFunctions.Wildcard -> None
        case PropertyValuePattern.Value(value) =>
          model.PropertyComparisonFunctions.Identicality -> Some(model.PropertyValue(value))
        case PropertyValuePattern.AnyValueExcept(value) =>
          model.PropertyComparisonFunctions.NonIdenticality -> Some(model.PropertyValue(value))
        case PropertyValuePattern.NoValue =>
          model.PropertyComparisonFunctions.NoValue -> None
        case PropertyValuePattern.RegexMatch(pattern) =>
          model.PropertyComparisonFunctions.RegexMatch(pattern.pattern) -> None
      }.toMap

      val localPropsWithLabels = if (labels.nonEmpty) {
        val labelSet = labels.map(qv => QuineValue.Str(qv.name)).toSet[QuineValue]
        val func = model.PropertyComparisonFunctions.ListContains(labelSet)
        localProps + (labelsProperty -> (func -> None))
      } else {
        localProps
      }

      val domainNodeEquiv = model.DomainNodeEquiv(
        className = None,
        localPropsWithLabels,
        circularEdges.result()
      )

      model.SingleBranch(domainNodeEquiv, qidOpt, domainEdges.result())
    }

    val query = synthesizeBranch(startingPoint)

    if (remainingNodes.nonEmpty) {
      throw InvalidQueryPattern("Pattern is not connected")
    } else {
      query -> returnColumn
    }
  }

  /* TODO: this is almost directly a copy-paste of `compiledDomainGraphBranch`,
   * but it really shouldn't. The reason why is that Cypher standing queries do
   * not have most of the restriction that DGB does: in particular, we can
   * support non-tree graphs! (just unfold into a tree and do a filter asserting
   * IDs match)
   */
  @throws[InvalidQueryPattern]
  def compiledCypherStandingQuery(
    labelsProperty: Symbol,
    idProvider: QuineIdProvider
  ): cypher.StandingQuery = {

    if (nodes.isEmpty) {
      throw InvalidQueryPattern("Pattern must be non-empty")
    }

    val watchedProperties: Map[NodePatternId, Map[Symbol, Symbol]] = toExtract
      .collect { case p: ReturnColumn.Property => p }
      .groupBy(_.node)
      .view
      .mapValues { props =>
        props.map { case ReturnColumn.Property(_, key, pat) => key -> pat }.toMap
      }
      .toMap

    val watchedIds: Map[NodePatternId, Map[Boolean, Symbol]] = toExtract.view
      .collect { case r: ReturnColumn.Id => r }
      .groupBy(_.node)
      .view
      .mapValues { ids =>
        ids.map { case ReturnColumn.Id(_, asStr, pat) => asStr -> pat }.toMap
      }
      .toMap

    // Keep track of which bits of the pattern are still unexplored
    val remainingNodes = mutable.Map.apply(nodes.map(pat => pat.id -> pat): _*)
    var remainingEdges = edges

    // Extract a query rooted at the given pattern
    def synthesizeQuery(id: NodePatternId): cypher.StandingQuery = {
      val subQueries = ArraySeq.newBuilder[cypher.StandingQuery]

      val NodePattern(_, labelOpt, qidOpt, props) = remainingNodes.remove(id).getOrElse {
        throw InvalidQueryPattern("Pattern has a cycle")
      }

      // Sub-queries for local properties
      for ((propKey, propPattern) <- props) {
        val alias = watchedProperties.get(id).flatMap(_.get(propKey))
        propPattern match {
          case PropertyValuePattern.AnyValue =>
            subQueries += cypher.StandingQuery.LocalProperty(
              propKey,
              cypher.StandingQuery.LocalProperty.Any,
              alias
            )
          case PropertyValuePattern.Value(value) =>
            val cypherValue = cypher.Expr.fromQuineValue(value)
            subQueries += cypher.StandingQuery.LocalProperty(
              propKey,
              cypher.StandingQuery.LocalProperty.Equal(cypherValue),
              alias
            )
          case PropertyValuePattern.AnyValueExcept(value) =>
            val cypherValue = cypher.Expr.fromQuineValue(value)
            subQueries += cypher.StandingQuery.LocalProperty(
              propKey,
              cypher.StandingQuery.LocalProperty.NotEqual(cypherValue),
              alias
            )
          case PropertyValuePattern.NoValue =>
            subQueries += cypher.StandingQuery.LocalProperty(
              propKey,
              cypher.StandingQuery.LocalProperty.None,
              alias
            )
          case PropertyValuePattern.RegexMatch(pattern) =>
            subQueries += cypher.StandingQuery.LocalProperty(
              propKey,
              cypher.StandingQuery.LocalProperty.Regex(pattern.pattern),
              alias
            )
        }
      }

      for (
        (propKey, alias) <- watchedProperties.getOrElse(id, Map.empty)
        if !props.contains(propKey)
      )
        subQueries += cypher.StandingQuery.LocalProperty(
          propKey,
          cypher.StandingQuery.LocalProperty.Any,
          Some(alias)
        )

      // Sub-queries for labels
      // TODO: add a special case for this in `cypher.StandingQuery.LocalProperty`
      labelOpt.foreach { label =>
        val labelTempVar = Symbol("__label")
        val labelListTempVar = Symbol("__label_list")

        subQueries += cypher.StandingQuery.FilterMap(
          condition = Some(
            cypher.Expr.AnyInList(
              variable = labelTempVar,
              list = cypher.Expr.Variable(labelListTempVar),
              filterPredicate = cypher.Expr.Equal(
                cypher.Expr.Variable(labelTempVar),
                cypher.Expr.fromQuineValue(QuineValue.Str(label.name))
              )
            )
          ),
          dropExisting = true,
          toFilter = cypher.StandingQuery.LocalProperty(
            propKey = labelsProperty,
            propConstraint = cypher.StandingQuery.LocalProperty.Any,
            aliasedAs = Some(labelListTempVar)
          ),
          toAdd = Nil
        )
      }
      qidOpt.foreach { qid =>
        val nodeIdTempVar = Symbol("__local_id")
        subQueries += cypher.StandingQuery.FilterMap(
          condition = Some(
            cypher.Expr.Equal(
              cypher.Expr.Variable(nodeIdTempVar),
              cypher.Expr.fromQuineValue(idProvider.qidToValue(qid))
            )
          ),
          dropExisting = true,
          toFilter = cypher.StandingQuery.LocalId(nodeIdTempVar, formatAsString = false),
          toAdd = Nil
        )
      }

      // Sub-queries for a local ID
      for ((formatAsString, aliasId) <- watchedIds.getOrElse(id, Map.empty))
        subQueries += cypher.StandingQuery.LocalId(aliasId, formatAsString)

      // sub-queries for edges
      val (connectedEdges, otherEdges) = remainingEdges.partition(e => e.from == id || e.to == id)
      remainingEdges = otherEdges

      for (EdgePattern(from, to, isDirected, label) <- connectedEdges) {
        val (other, edgeDir) = if (from == id) {
          (to, if (isDirected) EdgeDirection.Outgoing else EdgeDirection.Undirected)
        } else {
          (from, if (isDirected) EdgeDirection.Incoming else EdgeDirection.Undirected)
        }
        subQueries += cypher.StandingQuery.SubscribeAcrossEdge(
          edgeName = Some(label),
          edgeDirection = Some(edgeDir),
          andThen = synthesizeQuery(other)
        )
      }

      subQueries.result() match {
        case ArraySeq() => cypher.StandingQuery.UnitSq()
        case ArraySeq(singleQuery) => singleQuery
        case manyQueries => cypher.StandingQuery.Cross(manyQueries, emitSubscriptionsLazily = true)
      }
    }

    var query = synthesizeQuery(startingPoint)

    // If we filter or map, insert a `FilterMap`
    if (filterCond.nonEmpty || toReturn.nonEmpty) {
      query = cypher.StandingQuery.FilterMap(
        filterCond,
        query,
        dropExisting = toReturn.nonEmpty,
        toAdd = toReturn.toList
      )
    }

    if (remainingNodes.nonEmpty) {
      throw InvalidQueryPattern("Pattern is not connected")
    } else {
      query
    }
  }
}

object GraphQueryPattern {

  /** Unique identifier for a node in the graph 'pattern'.
    *
    * This has no bearing on IDs in Quine - it is just a mechanism for encoding
    * the graph pattern
    */
  final case class NodePatternId(id: Int) extends AnyVal

  /** Pattern for a node in the standing query graph
    *
    * @param id the ID of the node pattern
    * @param qidOpt the graph ID of the node, if the user enforced it
    * @param properties the properties expected to be on the node
    */
  final case class NodePattern(
    id: NodePatternId,
    labels: Set[Symbol],
    qidOpt: Option[QuineId],
    properties: Map[Symbol, PropertyValuePattern]
  )

  /** The sort of pattern we can express on a node in a graph standing query */
  sealed abstract class PropertyValuePattern
  object PropertyValuePattern {
    final case class Value(value: QuineValue) extends PropertyValuePattern
    final case class AnyValueExcept(value: QuineValue) extends PropertyValuePattern
    final case class RegexMatch(pattern: Pattern) extends PropertyValuePattern {
      override def equals(other: Any): Boolean = other match {
        case RegexMatch(otherPattern) => pattern.pattern == otherPattern.pattern
        case _ => false
      }
    }
    case object AnyValue extends PropertyValuePattern
    case object NoValue extends PropertyValuePattern
  }

  /** Pattern for an edge in the standing query graph
    *
    * @param from node pattern on one end of the edge
    * @param to node pattern on the other end of the edge
    * @param isDirected is the edge directed
    * @param label edge's label
    */
  final case class EdgePattern(
    from: NodePatternId,
    to: NodePatternId,
    isDirected: Boolean,
    label: Symbol
  )

  /** The sort of thing to extract
    */
  sealed abstract class ReturnColumn {
    val aliasedAs: Symbol
  }
  object ReturnColumn {

    /** @param node from which node in the pattern should the ID be returned?
      * @param formatAsString should the ID be an `strId(n)` or `id(n)`?
      * @param aliasedAs under which name should the result be returned?
      */
    final case class Id(
      node: NodePatternId,
      formatAsString: Boolean,
      aliasedAs: Symbol
    ) extends ReturnColumn

    final case class Property(
      node: NodePatternId,
      propertyKey: Symbol,
      aliasedAs: Symbol
    ) extends ReturnColumn
  }
}
