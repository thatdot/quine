package com.thatdot.quine.graph.cypher

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

import akka.NotUsed
import akka.actor.{Actor, ActorRef}
import akka.stream.scaladsl.Source
import akka.util.Timeout

import com.google.common.collect.MinMaxPriorityQueue

import com.thatdot.quine.graph.NodeChangeEvent.{EdgeAdded, EdgeRemoved, PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.cypher.Query._
import com.thatdot.quine.graph.messaging.CypherMessage.{CheckOtherHalfEdge, QueryContextResult, QueryPackage}
import com.thatdot.quine.graph.messaging.LiteralMessage.{DeleteNodeCommand, RemoveHalfEdgeCommand}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineRefOps}
import com.thatdot.quine.graph.{BaseNodeActor, CypherOpsGraph, NodeChangeEvent}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, PropertyValue, QuineId, QuineValue}

// Knows what to do with index anchored queries
// INV: Thread-safe
trait AnchoredInterpreter extends CypherInterpreter[Location.Anywhere] {

  def node: Option[BaseNodeActor] = None

  implicit val self: ActorRef = ActorRef.noSender

  final def interpret(
    query: Query[Location.Anywhere],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] =
    try query match {
      case query: Empty => interpretEmpty(query, context)
      case query: Unit => interpretUnit(query, context)
      case query: AnchoredEntry => interpretAnchoredEntry(query, context)
      case query: ArgumentEntry => interpretArgumentEntry(query, context)
      case query: LoadCSV => interpretLoadCSV(query, context)
      case query: Union[Location.Anywhere @unchecked] => interpretUnion(query, context)
      case query: Or[Location.Anywhere @unchecked] => interpretOr(query, context)
      case query: ValueHashJoin[Location.Anywhere @unchecked] =>
        interpretValueHashJoin(query, context)
      case query: SemiApply[Location.Anywhere @unchecked] => interpretSemiApply(query, context)
      case query: Apply[Location.Anywhere @unchecked] => interpretApply(query, context)
      case query: Optional[Location.Anywhere @unchecked] => interpretOptional(query, context)
      case query: Filter[Location.Anywhere @unchecked] => interpretFilter(query, context)
      case query: Skip[Location.Anywhere @unchecked] => interpretSkip(query, context)
      case query: Limit[Location.Anywhere @unchecked] => interpretLimit(query, context)
      case query: Sort[Location.Anywhere @unchecked] => interpretSort(query, context)
      case query: Top[Location.Anywhere @unchecked] => interpretTop(query, context)
      case query: Distinct[Location.Anywhere @unchecked] => interpretDistinct(query, context)
      case query: Unwind[Location.Anywhere @unchecked] => interpretUnwind(query, context)
      case query: AdjustContext[Location.Anywhere @unchecked] =>
        interpretAdjustContext(query, context)
      case query: EagerAggregation[Location.Anywhere @unchecked] =>
        interpretEagerAggregation(query, context)
      case query: Delete => interpretDelete(query, context)
      case query: ProcedureCall => interpretProcedureCall(query, context)
    } catch {
      case NonFatal(e) => Source.failed(e)
    }

  // TODO: do these properly!!!
  implicit private val t: Timeout = Timeout(30.seconds)

  final private[cypher] def interpretAnchoredEntry(
    query: AnchoredEntry,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {

    val qids: Source[QuineId, Any] = query.entry match {
      case EntryPoint.AllNodesScan =>
        graph.enumerateAllNodeIds()

      case EntryPoint.NodeById(ids) =>
        Source.fromIterator(() => ids.iterator)
    }
    qids.flatMapConcat { (qid: QuineId) =>
      Source
        .futureSource(qid ? (QueryPackage(query.andThen, parameters, context, _)))
        .map(_.result)
    }
  }

  final private[cypher] def interpretArgumentEntry(
    query: ArgumentEntry,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    val other: QuineId = getQuineId(query.node.eval(context)) match {
      case Some(other) => other
      case None => return Source.empty
    }

    Source
      .lazyFutureSource(() => other ? (QueryPackage(query.andThen, parameters, context, _)))
      .map(_.result)
  }
}

// Knows what to do with in-node queries
trait OnNodeInterpreter
    extends CypherInterpreter[Location.OnNode]
    with Actor
    with BaseNodeActor
    with QuineIdOps
    with QuineRefOps {

  def node: Option[BaseNodeActor] = Some(this)

  final def interpret(
    query: Query[Location.OnNode],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] =
    try query match {
      case query: Empty => interpretEmpty(query, context)
      case query: Unit => interpretUnit(query, context)
      case query: AnchoredEntry => interpretAnchoredEntry(query, context)
      case query: ArgumentEntry => interpretArgumentEntry(query, context)
      case query: Expand => interpretExpand(query, context)
      case query: LocalNode => interpretLocalNode(query, context)
      case query: GetDegree => interpretGetDegree(query, context)
      case query: LoadCSV => interpretLoadCSV(query, context)
      case query: Union[Location.OnNode @unchecked] => interpretUnion(query, context)
      case query: Or[Location.OnNode @unchecked] => interpretOr(query, context)
      case query: ValueHashJoin[Location.OnNode @unchecked] =>
        interpretValueHashJoin(query, context)
      case query: SemiApply[Location.OnNode @unchecked] => interpretSemiApply(query, context)
      case query: Apply[Location.OnNode @unchecked] => interpretApply(query, context)
      case query: Optional[Location.OnNode @unchecked] => interpretOptional(query, context)
      case query: Filter[Location.OnNode @unchecked] => interpretFilter(query, context)
      case query: Skip[Location.OnNode @unchecked] => interpretSkip(query, context)
      case query: Limit[Location.OnNode @unchecked] => interpretLimit(query, context)
      case query: Sort[Location.OnNode @unchecked] => interpretSort(query, context)
      case query: Top[Location.OnNode @unchecked] => interpretTop(query, context)
      case query: Distinct[Location.OnNode @unchecked] => interpretDistinct(query, context)
      case query: Unwind[Location.OnNode @unchecked] => interpretUnwind(query, context)
      case query: AdjustContext[Location.OnNode @unchecked] =>
        interpretAdjustContext(query, context)
      case query: SetProperty => interpretSetProperty(query, context)
      case query: SetProperties => interpretSetProperties(query, context)
      case query: SetEdge => interpretSetEdge(query, context)
      case query: SetLabels => interpretSetLabels(query, context)
      case query: EagerAggregation[Location.OnNode @unchecked] =>
        interpretEagerAggregation(query, context)
      case query: Delete => interpretDelete(query, context)
      case query: ProcedureCall => interpretProcedureCall(query, context)
    } catch {
      case NonFatal(e) => Source.failed(e)
    }

  final private def labelsProperty: Symbol = graph.labelsProperty

  // TODO: do these properly!!!
  implicit private val t: Timeout = Timeout(30.seconds)

  def graph: CypherOpsGraph

  /* By the time we get to interpreting the inner query (possibly multiple
   * times), the node will have moved on to processing other messages. It is
   * therefore critical to explicitly queue up the query back in the node's
   * mailbox if there is any chance the query will touch node state.
   */
  override private[quine] def interpretRecursive(
    query: Query[Location.OnNode],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] =
    if (query.canDirectlyTouchNode) {
      Source
        .lazyFutureSource[QueryContextResult, akka.NotUsed] { () =>
          qidAtTime ? (QueryPackage(query, parameters, context, _))
        }
        .map(_.result)
    } else {
      interpret(query, context)
    }

  final private[cypher] def interpretAnchoredEntry(
    query: AnchoredEntry,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] =
    graph.cypherOps.query(query, parameters, atTime, context)

  final private[cypher] def interpretArgumentEntry(
    query: ArgumentEntry,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    val other: QuineId = getQuineId(query.node.eval(context)) match {
      case Some(other) => other
      case None => return Source.empty
    }

    if (other == qid) {
      interpret(query.andThen, context)
    } else {
      Source
        .lazyFutureSource { () =>
          other ? (QueryPackage(query.andThen, parameters, context, _))
        }
        .map(_.result)
    }
  }

  final private[cypher] def interpretExpand(
    expand: Expand,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {

    val myQid = qid
    val Expand(edgeName, toNode, _, bindRelation, range, visited, andThen, _) = expand

    val maxExpandVisitedCount = 1000 // TODO move elsewhere
    if (visited.size > maxExpandVisitedCount) {
      throw CypherException.Runtime(
        s"Variable length relationship pattern exceeded maximum traversal length $maxExpandVisitedCount (update upper bound of length in relationship pattern)"
      )
    }

    /* There is no such thing as an undirected edge in Cypher: `(n)--(m)` means
     * either `(n)-->(m)` or `(n)<--(m)`
     */
    val direction: Option[EdgeDirection] = expand.direction match {
      case EdgeDirection.Undirected => None
      case directed => Some(directed)
    }

    /* Compute the other end of the edge, if available */
    val literalFarNodeId: Option[QuineId] = toNode map { (toNode: Expr) =>
      val otherVal = toNode.eval(context)
      getQuineId(otherVal) getOrElse {
        throw CypherException.TypeMismatch(
          expected = Seq(Type.Node),
          actualValue = otherVal,
          context = "one extremity of an edge we are expanding to"
        )
      }
    }

    /* Get edges matching the direction / name constraint.
     *
     * As tempting as this may be, this _cannot_ be an iterator, else we close
     * over mutable node state (and multiple threads can concurrently access
     * edges).
     */
    val halfEdges: Vector[HalfEdge] = (edgeName, direction, literalFarNodeId) match {
      case (None, None, None) =>
        edges.all.toVector
      case (None, None, Some(id)) =>
        edges.matching(id).toVector
      case (None, Some(dir), None) =>
        edges.matching(dir).toVector
      case (None, Some(dir), Some(id)) =>
        edges.matching(dir, id).toVector
      case (Some(names), None, None) =>
        names.toVector.flatMap(edges.matching(_))
      case (Some(names), None, Some(id)) =>
        names.toVector.flatMap(edges.matching(_, id))
      case (Some(names), Some(dir), None) =>
        names.toVector.flatMap(edges.matching(_, dir))
      case (Some(names), Some(dir), Some(id)) =>
        names.toVector.flatMap(edges.matching(_, dir, id))
    }

    Source.fromIterator(() => halfEdges.iterator.filter(!visited.contains(myQid, _))).flatMapConcat {
      // Undirected edges don't exist for Cypher :)
      case HalfEdge(_, EdgeDirection.Undirected, _) => Source.empty

      case halfEdge @ HalfEdge(sym, dir, halfEdgeFarNode) =>
        val newContext = bindRelation match {
          case None => context

          // TODO: record properties
          case Some(asName) if range.isEmpty =>
            val rel = Expr.Relationship(myQid, sym, Map.empty, halfEdgeFarNode)
            val rel2 = if (dir == EdgeDirection.Outgoing) rel else rel.reflect
            context + (asName -> rel2)

          case Some(asName) =>
            context + (asName -> Expr.List(visited.addEdge(myQid, halfEdge).relationships))
        }

        // source that produces the result of running the andThen query on the remote node
        lazy val andThenSource: Source[QueryContext, Future[NotUsed]] = Source
          .futureSource {
            halfEdgeFarNode ? (ref =>
              CheckOtherHalfEdge(
                halfEdge = halfEdge.reflect(myQid),
                action = None,
                query = andThen,
                parameters = parameters,
                context = newContext,
                replyTo = ref
              )
            )
          }
          .map(_.result)

        // source that produces the result of recursively running this expand query on the remote node
        lazy val recursiveExpandSource: Source[QueryContext, Future[NotUsed]] = Source
          .futureSource {
            halfEdgeFarNode ? (ref =>
              CheckOtherHalfEdge(
                halfEdge = halfEdge.reflect(myQid),
                action = None,
                query = expand.copy(
                  visited = visited.addEdge(myQid, halfEdge)
                ),
                parameters = parameters,
                context = context,
                replyTo = ref
              )
            )
          }
          .map(_.result)

        range match {
          case None => andThenSource
          case Some(range) =>
            // Match the far node (if it is in range)
            val andThenMatch =
              if (
                range match {
                  case (Some(lower), None) => visited.size + 1L >= lower
                  case (None, Some(upper)) => visited.size + 1L <= upper
                  case (Some(lower), Some(upper)) => visited.size + 1L >= lower && visited.size + 1L <= upper
                  case (None, None) => false
                }
              ) andThenSource
              else Source.empty
            // Recursively expand the same query for a variable-length edge
            // (if relatives of the far node will be in range)
            val recursiveMatch = {
              if (
                range match {
                  case (_, Some(upper)) => visited.size + 2L <= upper
                  case (_, None) => true
                }
              ) recursiveExpandSource
              else Source.empty
            }
            andThenMatch ++ recursiveMatch
        }
    }
  }

  final private[cypher] def interpretLocalNode(
    query: LocalNode,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    val requiredPropsOpt: Option[Map[String, Value]] = query.propertiesOpt.map { expr =>
      expr.eval(context) match {
        case Expr.Map(map) => map
        case other =>
          throw CypherException.TypeMismatch(
            expected = Seq(Type.Map),
            actualValue = other,
            context = "property map to check on a node"
          )
      }
    }

    val cypherProps: Map[Symbol, Value] = properties.view.flatMap { case (key, value) =>
      value.deserialized.toOption.map(v => key -> Expr.fromQuineValue(v))
    }.toMap

    // Weed out cases where the node is missing a required property values
    def missingRequiredProp = requiredPropsOpt.exists { requiredProps =>
      requiredProps.exists { case (key, expectedValue) =>
        !cypherProps.get(Symbol(key)).exists(_ == expectedValue)
      }
    }
    if (missingRequiredProp) {
      return Source.empty
    }

    // Get all of the labels on the node
    val labels = getLabels() match {
      case Some(lbls) => lbls
      case None => return Source.empty // TODO: should we error/warn here?
    }

    // Check whether the node has the required labels
    if (query.labelsOpt.exists(expectedLabels => !expectedLabels.toSet.subsetOf(labels))) {
      return Source.empty
    }

    val newContext = query.bindName match {
      case None => context
      case Some(asName) =>
        val realProperties = cypherProps - labelsProperty
        context + (asName -> Expr.Node(qid, labels, realProperties))
    }
    Source.single(newContext)
  }

  // TODO: check the other end of half edges?
  final private[cypher] def interpretGetDegree(
    query: GetDegree,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    val degree: Int = query.edgeName match {
      case None => edges.matching(query.direction).size
      case Some(n) => edges.matching(n, query.direction).size
    }

    val newContext = context + (query.bindName -> Expr.Integer(degree.toLong))
    Source.single(newContext)
  }

  final private[cypher] def interpretSetProperty(
    query: SetProperty,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    val event = query.newValue match {
      case None => PropertyRemoved(query.key, PropertyValue(QuineValue.Null))
      case Some(expr) => PropertySet(query.key, PropertyValue(Expr.toQuineValue(expr.eval(context))))
    }
    Source
      .future(processEvent(event))
      .map(_ => context)
  }

  final private[cypher] def interpretSetProperties(
    query: SetProperties,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    val map: Map[Symbol, Value] = query.properties.eval(context) match {
      case Expr.Map(map) => map.map { case (k, v) => Symbol(k) -> v }.toMap
      case Expr.Node(_, _, props) => props
      case Expr.Relationship(_, _, props, _) => props
      case otherVal =>
        throw CypherException.TypeMismatch(
          expected = Seq(Type.Map, Type.Node, Type.Relationship),
          actualValue = otherVal,
          context = "properties set on node"
        )
    }

    // Build up the full set to events to process before processing them
    val eventsToProcess = Vector.newBuilder[NodeChangeEvent]

    // Optionally drop existing properties
    if (!query.includeExisting) {
      for (key <- properties.keys)
        if (!(map.contains(key) || labelsProperty == key)) {
          eventsToProcess += PropertyRemoved(key, PropertyValue(QuineValue.Null))
        }
    }

    // Add all the new properties
    for ((key, value) <- map)
      eventsToProcess += PropertySet(key, PropertyValue(Expr.toQuineValue(value)))

    Source.future(Future.traverse(eventsToProcess.result())(processEvent(_)).map(_ => context))
  }

  final private[quine] def interpretSetEdge(
    query: SetEdge,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    // Figure out what the other end of the edge is
    val otherVal = query.target.eval(context)
    val other: QuineId = getQuineId(otherVal).getOrElse {
      throw CypherException.TypeMismatch(
        expected = Seq(Type.Node),
        actualValue = otherVal,
        context = "one extremity of an edge we are modifying"
      )
    }

    // Add the half-edge locally
    val edge: HalfEdge = HalfEdge(query.label, query.direction, other)
    val event = if (query.add) EdgeAdded(edge) else EdgeRemoved(edge)
    val setThisHalf = processEvent(event)

    val newContext = query.bindRelation match {
      case None => context

      // TODO: record properties
      case Some(asName) =>
        val rel = Expr.Relationship(qid, query.label, Map.empty, other)
        val rel2 = if (query.direction == EdgeDirection.Outgoing) rel else rel.reflect
        context + (asName -> rel2)
    }

    // Rest of the query (along with instructions for the other half edge)
    val setOtherHalf = other ? (CheckOtherHalfEdge(
      halfEdge = edge.reflect(qid),
      action = Some(query.add),
      query = query.andThen,
      parameters,
      newContext,
      _
    ))

    Source
      .futureSource(setThisHalf.flatMap(_ => setOtherHalf))
      .map(_.result)
  }

  final private[quine] def interpretSetLabels(
    query: SetLabels,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    // get current label value
    val currentLabelValue = getLabels() match {
      case Some(lbls) => lbls
      case None => return Source.empty // TODO: should we error/warn here?
    }

    // Compute new label value
    val newLabelValue = if (query.add) {
      currentLabelValue ++ query.labels
    } else {
      currentLabelValue -- query.labels
    }

    // Set new label value
    val setLabelsFut = setLabels(newLabelValue)
    Source.future(setLabelsFut.map(_ => context))
  }
}

trait CypherInterpreter[Start <: Location] extends ProcedureExecutionLocation {

  import Query._

  // TODO: do this properly!!!
  implicit private val t: Timeout = Timeout(30.seconds)

  /** Interpret a Cypher query into a [[Source]] of query results
    *
    * @note a [[Source]] can be run many times (possible 0 times), so this method is really just
    * creating 'instructions' for running the query as opposed to actually running it
    *
    * @param query Cypher query
    * @param context variables in scope
    * @param ec execution context
    * @param parameters query constants in scope
    * @return back-pressured source of results
    */
  def interpret(
    query: Query[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _]

  /** When calling [[interpret]] recursively, if the call is not being done
    * synchoronously, use [[interpretRecursive]] instead. For instance:
    *
    * {{{
    * // `interpret` is called synchronously
    * interpret(myQuery.subQuery1).flatMapConcat { x =>
    *
    *   // `interpretRecursive` will be called asynchronously as the stream runs!
    *   interpretRecursive(myQuery.subQuery2)
    * }
    */
  private[quine] def interpretRecursive(
    query: Query[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = interpret(query, context)

  private object ValueQid {
    def unapply(value: Value): Option[QuineId] = for {
      quineValue <- Try(Expr.toQuineValue(value)).toOption
      quineId <- idProvider.valueToQid(quineValue)
    } yield quineId
  }

  /** Try to pull a node ID from an expression
    *
    * @return ID extracted from expression
    */
  final private[quine] def getQuineId(expr: Value): Option[QuineId] = expr match {
    case Expr.Node(other, _, _) => Some(other)
    case ValueQid(qid) => Some(qid)

    // TODO: are these honest? (they _are_ user visible - `MATCH (n) WHERE id(n) = bytes("CAFEBABE") RETURN n`)
    case Expr.Bytes(id, representsId @ _) => Some(QuineId(id)) // used by `FreshNodeId`

    // TODO: find a more principled way to do this, see [[IdFunc]]
    case Expr.Str(strId) =>
      idProvider.qidFromPrettyString(strId) match {
        case Failure(_) => None
        case Success(qid) => Some(qid)
      }

    case _ => None
  }

  final private[quine] def interpretEmpty(
    query: Empty,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = Source.empty

  final private[quine] def interpretUnit(
    query: Unit,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = Source.single(context)

  final private[quine] def interpretLoadCSV(
    query: LoadCSV,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    def splitCols(line: String): Array[String] = {
      val rowBuilder = Array.newBuilder[String]
      val cellBuilder = new mutable.StringBuilder()
      var inQuoted: Boolean = false
      val characterIterator = line.iterator

      while (characterIterator.hasNext)
        characterIterator.next() match {
          case '"' if inQuoted =>
            if (!characterIterator.hasNext) {
              inQuoted = false
              rowBuilder += cellBuilder.result()
              return rowBuilder.result()
            }
            characterIterator.next() match {
              case '"' =>
                cellBuilder += '"'

              case c if c == query.fieldTerminator =>
                inQuoted = false
                rowBuilder += cellBuilder.result()
                cellBuilder.clear()

              // TODO: warn on this state?
              case c =>
                inQuoted = false
                cellBuilder += c
            }

          case '"' =>
            inQuoted = true

          case c if !inQuoted && c == query.fieldTerminator =>
            rowBuilder += cellBuilder.result()
            cellBuilder.clear()

          case c =>
            cellBuilder += c
        }

      rowBuilder += cellBuilder.result()
      rowBuilder.result()
    }

    val url: String = query.urlString.eval(context).asString("LOAD CSV clause")
    val lineIterator = scala.io.Source.fromURL(url).getLines()

    val csvRows: Source[QueryContext, _] = if (query.withHeaders) {
      val headerLine: Array[String] = splitCols(lineIterator.next())
      Source.fromIterator(() =>
        lineIterator.map { (line: String) =>
          val lineMap = Expr.Map {
            headerLine
              .zip(splitCols(line))
              .map { case (header, value) => header -> Expr.Str(value) }
              .toMap
          }
          context + (query.variable -> lineMap)
        }
      )
    } else {
      Source.fromIterator(() =>
        lineIterator.map { (line: String) =>
          val lineList = Expr.List {
            splitCols(line).toVector.map(Expr.Str)
          }
          context + (query.variable -> lineList)
        }
      )
    }

    csvRows
  }

  final private[quine] def interpretUnion(
    query: Union[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    val lhsResult = interpret(query.unionLhs, context)
    val rhsResult = interpret(query.unionRhs, context)
    lhsResult ++ rhsResult
  }

  final private[quine] def interpretOr(
    query: Or[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    val lhsResult = interpret(query.tryFirst, context)
    val rhsResult = interpret(query.trySecond, context)
    lhsResult orElse rhsResult
  }

  final private[quine] def interpretSemiApply(
    query: SemiApply[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    val results = interpret(query.acceptIfThisSucceeds, context)
    val keepFut = query.inverted match {
      case false => results.take(1).fold(false)((_acc, _other) => true)
      case true => results.take(1).fold(true)((_acc, _other) => false)
    }
    keepFut.flatMapConcat {
      case true => Source.single(context)
      case false => Source.empty
    }
  }

  final private[cypher] def interpretApply(
    query: Apply[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] =
    interpret(query.startWithThis, context)
      .flatMapConcat(interpretRecursive(query.thenCrossWithThis, _))

  final private[quine] def interpretValueHashJoin(
    query: ValueHashJoin[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    val lhsResults = interpret(query.joinLhs, context)
    val rhsResults = interpret(query.joinRhs, context)

    lhsResults
      .fold(Map.empty[Value, List[QueryContext]]) { (acc, qc) =>
        val key = query.lhsProperty.eval(qc)
        val value = qc :: acc.getOrElse(key, List.empty)
        acc + (key -> value)
      }
      .flatMapConcat { (leftMap: Map[Value, List[QueryContext]]) =>
        rhsResults.mapConcat { (newContext: QueryContext) =>
          val rhsVal = query.rhsProperty.eval(newContext)
          val matchingProp = leftMap.getOrElse(rhsVal, List.empty).map(_ ++ newContext)
          matchingProp
        }
      }
  }

  final private[quine] def interpretOptional(
    query: Optional[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    val result = interpret(query.query, context)
    result.orElse(Source.single(context))
  }

  final private[quine] def interpretFilter(
    query: Filter[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] =
    interpret(query.toFilter, context).filter { (qc: QueryContext) =>
      /* This includes boolean expressions that are used as predicates in the
       * `WHERE` clause. In this case, anything that is not true is interpreted
       * as being false.
       */
      query.condition.eval(qc) match {
        case Expr.True => true
        case Expr.List(l) => l.nonEmpty
        case _ => false
      }
    }

  final private[quine] def interpretSkip(
    query: Skip[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    // TODO: type error if number is not positive
    val skip = query.drop.eval(context).asLong("SKIP clause")
    interpret(query.toSkip, context).drop(skip)
  }

  final private[quine] def interpretLimit(
    query: Limit[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    // TODO: type error if number is not positive
    val limit = query.take.eval(context).asLong("LIMIT clause")
    interpret(query.toLimit, context).take(limit)
  }

  final private[quine] def interpretSort(
    query: Sort[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    val sourceToSort = interpret(query.toSort, context)

    // We need lazily to ensure that we don't re-use `priorityQueue` across materializations
    Source.lazySource { () =>
      // The ordering will evaluate the query context on all columns
      val priorityQueue = collection.mutable.PriorityQueue.empty(QueryContext.orderingBy(query.by))

      sourceToSort
        .fold(priorityQueue)(_ += _)
        .flatMapConcat { queue =>
          Source.fromIterator(() =>
            new Iterator[QueryContext] {
              def hasNext = priorityQueue.nonEmpty
              def next() = priorityQueue.dequeue()
            }
          )
        }
    }
  }

  final private[quine] def interpretTop(
    query: Top[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    val capacity = query.limit.eval(context).asLong("TOP clause")
    val sourceToTop = interpret(query.toTop, context)

    // We need lazily to ensure that we don't re-use `priorityQueue` across materializations
    Source.lazySource { () =>
      // The `maximumSize` evicts the largest element whenever the queue gets too big
      // The ordering is inverted so smaller elements appear larger (and get evicted first)
      val priorityQueue: MinMaxPriorityQueue[QueryContext] = MinMaxPriorityQueue
        .orderedBy(QueryContext.orderingBy(query.by).reversed)
        .maximumSize(capacity.toInt)
        .create()

      sourceToTop
        .fold(priorityQueue) { (queue, elem) => queue.add(elem); queue }
        .flatMapConcat { queue =>
          Source
            .fromIterator(() =>
              new Iterator[QueryContext] {
                def hasNext = !queue.isEmpty
                def next = queue.removeFirst
              }
            )
            .take(capacity)
        }
    }
  }

  final private[quine] def interpretDistinct(
    query: Distinct[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    val sourceToDedup = interpret(query.toDedup, context)

    // We need lazily to ensure that we don't re-use `seen` across materializations
    Source.lazySource { () =>
      val seen = collection.mutable.Set.empty[Seq[Value]]

      sourceToDedup.filter { (qc: QueryContext) =>
        seen.add(query.by.map(_.eval(qc)))
      }
    }
  }

  final private[quine] def interpretUnwind(
    query: Unwind[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {

    /* Deciding how to unwind the value is a peculiar process. The Neo4j Cypher
     * manual claims that unwinding anything that is not [[Expr.Null]] or
     * [[Expr.List]] should result in an error. However, on the same page, they
     * give the following example:
     *
     * ```
     * WITH \[\[1, 2\],\[3, 4\], 5\] AS nested
     * UNWIND nested AS x
     * UNWIND x AS y       // At some point, 5 goes through here and returns 5!
     * RETURN y
     * ==> [ { y: 1 }, { y: 2 }, { y: 3 }, { y: 4 }, { y: 5 } ]
     * ```
     *
     * Alec's interpretation of the manual is as follows: if Cypher can detect
     * at query planning time that `UNWIND` is receiving a non-list, it will
     * produce an error. If not, the runtime will unwind any invalid value to
     * a one row output containing just the value.
     */
    val list: Vector[Value] = query.listExpr.eval(context) match {
      case Expr.Null => Vector()
      case Expr.List(l) => l
      case path: Expr.Path => path.toList.list
      case otherVal => Vector(otherVal) // see above comment for why this isn't a type error
    }

    Source(list)
      .map((elem: Value) => context + (query.as -> elem))
      .flatMapConcat(interpretRecursive(query.unwindFrom, _))
  }

  private[quine] def interpretAdjustContext(
    query: AdjustContext[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] =
    interpret(query.adjustThis, context).map { (qc: QueryContext) =>
      val removed = query.dropExisting match {
        case true => QueryContext.empty
        case false => qc
      }
      removed ++ QueryContext(query.toAdd.map { case (k, e) => k -> e.eval(qc) }.toMap)
    }

  /* I (Alec) find this aggregation behaviour somewhat un-intuitive. [Here is a
   * webpage that details the aggregating behaviour][0], hopefully convincing you
   * that this _is_ the correct behaviour
   *
   * [0]: https://neo4j.com/docs/cypher-manual/current/functions/aggregating/
   */
  final private[quine] def interpretEagerAggregation(
    query: EagerAggregation[Start],
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {
    val (criteriaSyms: Vector[Symbol], criteriaExprs: Vector[Expr]) = query.aggregateAlong.unzip
    val (aggregateSyms: Vector[Symbol], aggregators: Vector[Aggregator]) = query.aggregateWith.unzip
    val sourceToAggregate = interpret(query.toAggregate, context)

    /* This condition is subtle; unless we have at least one criteria along
     * which to group, _there will always be exactly one result_.
     *
     * Motivating example:
     *
     *   - `UNWIND [] AS N RETURN    count(*)` returns `[ { count(*): 0 } ]`
     *   - `UNWIND [] AS N RETURN N, count(*)` returns `[]`
     */
    if (criteriaSyms.isEmpty) {

      // We need lazily to ensure that we don't re-use `aggregatedStates` across materializations
      Source.lazySource { () =>
        val aggregatedStates = aggregators.map(_.aggregate())

        sourceToAggregate
          .fold(aggregatedStates) { (states, result) =>
            for (state <- states)
              state.visitRow(result)
            states
          }
          .map { aggregateValues =>
            val newCtx = QueryContext(
              aggregateSyms.zip(aggregateValues.map(_.result())).toMap
            )
            if (query.keepExisting) context ++ newCtx else newCtx
          }
      }
    } else {

      // We need lazily to ensure that we don't re-use `aggregatedStates` across materializations
      Source.lazySource { () =>
        val aggregatedStates = collection.mutable.Map.empty[Vector[Value], Vector[AggregateState]]

        sourceToAggregate
          .fold(aggregatedStates) { (buckets, result) =>
            val keys = criteriaExprs.map(_.eval(result))
            val states = buckets.getOrElseUpdate(keys, aggregators.map(_.aggregate()))
            for (state <- states)
              state.visitRow(result)
            buckets
          }
          .mapConcat { buckets =>
            buckets.toVector.map { case (criteriaValues, aggregateValues) =>
              val newCtx = QueryContext(
                criteriaSyms.zip(criteriaValues).toMap ++
                aggregateSyms.zip(aggregateValues.map(_.result()))
              )
              if (query.keepExisting) context ++ newCtx else newCtx
            }
          }
      }
    }
  }

  final private[quine] def interpretDelete(
    query: Delete,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] =
    query.toDelete.eval(context) match {
      case Expr.Null => Source.empty

      case Expr.Node(qid, _, _) =>
        val completed = (qid ? (DeleteNodeCommand(query.detach, _))).flatten
          .flatMap {
            case DeleteNodeCommand.Success => Future.successful(())
            case DeleteNodeCommand.Failed(n) =>
              Future.failed(
                CypherException.ConstraintViolation(
                  s"Node $qid cannot be deleted since it still has $n relationships."
                )
              )
          }
        Source.future(completed).map(_ => context)

      case Expr.Relationship(from, name, _, to) =>
        val he = HalfEdge(name, EdgeDirection.Outgoing, to)
        val firstHalf = (from ? (RemoveHalfEdgeCommand(he, _))).flatten
        val secondHalf = (to ? (RemoveHalfEdgeCommand(he.reflect(from), _))).flatten
        Source.future(firstHalf.zip(secondHalf)).map(_ => context)

      // case Expr.Path => TODO

      case otherVal =>
        throw CypherException.TypeMismatch(
          expected = Seq(Type.Node, Type.Relationship, Type.Path),
          actualValue = otherVal,
          context = "target for deletion"
        )
    }

  final private[quine] def interpretProcedureCall(
    query: ProcedureCall,
    context: QueryContext
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters
  ): Source[QueryContext, _] = {

    // Remap the procedure outputs and add existing input columns
    val makeResultRow: Vector[Value] => QueryContext = query.returns match {
      case None =>
        val variables = query.procedure.outputColumns.variables
        (outputs: Vector[Value]) => context ++ variables.view.zip(outputs.view)
      case Some(remaps) =>
        val indices: Vector[(Symbol, Int)] = remaps.view.map { case (orig, out) =>
          out -> query.procedure.outputColumns.variables.indexOf(orig)
        }.toVector
        (outputs: Vector[Value]) => context ++ indices.view.map { case (key, idx) => key -> outputs(idx) }
    }

    query.procedure
      .call(context, query.arguments.map(_.eval(context)), this)
      .map(makeResultRow)
  }
}
