package com.thatdot.quine.graph.cypher

import java.util.concurrent.ConcurrentHashMap

import scala.collection.compat._
import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

import akka.stream.scaladsl.Source
import akka.util.Timeout

import com.thatdot.quine.graph.messaging.{LiteralMessage, QuineIdAtTime}
import com.thatdot.quine.graph.{BaseGraph, LiteralOpsGraph}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, Milliseconds, QuineId}

/** Cypher procedure
  *
  * TODO: thread in type signatures and error messages
  */
sealed abstract class Proc {

  /** Output columns of the procedure */
  def outputColumns: Columns.Specified

  /** Can the procedure cause any updates? */
  def canContainUpdates: Boolean

  /** Is the procedure idempotent? See {Query} for full comment. */
  def isIdempotent: Boolean

  /** Can the procedure cause a full node scan? */
  def canContainAllNodeScan: Boolean

  /** Is the procedure a VOID procedure? */
  def isVoid = outputColumns.variables.isEmpty

  /** Call the procedure
    *
    * @see [[UserDefinedProcedure]]
    */
  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters,
    timeout: Timeout
  ): Source[Vector[Value], _]

}
object Proc {

  /** Custom user defined procedures which are registered at runtime
    * Keys must be lowercase!
    *
    * @note this must be kept in sync across the entire logical graph
    */
  final val userDefinedProcedures: concurrent.Map[String, UserDefinedProcedure] =
    new ConcurrentHashMap[String, UserDefinedProcedure]().asScala

  case object ShortestPath extends Proc {

    val name: String = "algorithms.shortestPath"
    val canContainUpdates: Boolean = false
    val isIdempotent: Boolean = true
    val canContainAllNodeScan: Boolean = false
    val retColumnPathName: Symbol = Symbol("path")
    val outputColumns: Columns.Specified = Columns.Specified(Vector(retColumnPathName))

    // TODO: abstract this out into a config setting
    val defaultMaxLength: Int = 10

    private def getNode(qid: QuineId, graph: BaseGraph, atTime: Option[Milliseconds])(implicit
      ec: ExecutionContext,
      timeout: Timeout
    ): Future[Expr.Node] =
      graph
        .relayAsk(QuineIdAtTime(qid, atTime), LiteralMessage.GetRawPropertiesCommand(_))
        .map { case LiteralMessage.RawPropertiesMap(labels, props) =>
          Expr.Node(
            qid,
            labels.getOrElse(Set.empty),
            props.view.mapValues(pv => Expr.fromQuineValue(pv.deserialized.get)).toMap
          )
        }

    def call(
      context: QueryContext,
      arguments: Seq[Value],
      location: ProcedureExecutionLocation
    )(implicit
      ec: ExecutionContext,
      parameters: Parameters,
      timeout: Timeout
    ): Source[Vector[Value], _] = {

      val (startNode, endNode, options): (QuineId, QuineId, Map[String, Value]) = arguments match {
        case Seq(n1: Expr.Node, n2: Expr.Node, Expr.Map(map)) => (n1.id, n2.id, map)
        case Seq(n1: Expr.Node, n2: Expr.Node) => (n1.id, n2.id, Map.empty)
        case other =>
          throw CypherException.WrongSignature(
            name,
            expectedArguments = Seq(Type.Node, Type.Node, Type.Map),
            actualArguments = other
          )
      }
      val graph = LiteralOpsGraph.getOrThrow(s"`$name` procedure", location.graph)
      val atTime = location.atTime

      // Get the valid edge directions in the path pattern
      val directionFilter: Option[EdgeDirection] = options.get("direction") match {
        case Some(Expr.Str("outgoing")) => Some(EdgeDirection.Outgoing)
        case Some(Expr.Str("incoming")) => Some(EdgeDirection.Incoming)
        case _ => None
      }

      // Get the min & max length of the path pattern
      val allowEmpty: Boolean = options
        .get("minLength")
        .collect { case Expr.Integer(0) => true }
        .getOrElse(false)
      val maxLength: Int = options
        .get("maxLength")
        .collect { case Expr.Integer(n) if n >= 0 => n.toInt }
        .getOrElse(defaultMaxLength)

      // Get valid edge types to traverse
      val edgeTypes: Option[Set[Symbol]] = options
        .get("types")
        .collect { case Expr.List(elems) =>
          elems.collect { case Expr.Str(lbl) => Symbol(lbl) }.toSet
        }

      /** Take a step to expand the search radius
        * @param seen nodes already visited
        * @param toExpand the search frontier to expand -- a Map from outermost qid to the path of
        *                 qids taken to reach that outermost qid
        * @param dirFilter the direction of half edges, if any, to consider
        * @return the next search frontier
        */
      def stepOutwards(
        seen: Set[QuineId],
        toExpand: Map[QuineId, List[(QuineId, Expr.Relationship)]],
        dirFilter: Option[EdgeDirection]
      ): Future[Map[QuineId, List[(QuineId, Expr.Relationship)]]] =
        Future
          .traverse(toExpand: Iterable[(QuineId, List[(QuineId, Expr.Relationship)])]) { case (qid, path) =>
            graph.literalOps
              .getHalfEdges(
                qid,
                // optimization: if we're only looking for the shortest path along a single edge
                // type, only query for half edges with that type.
                withType = edgeTypes.collect { case s if s.size == 1 => s.head },
                withDir = dirFilter,
                atTime = atTime
              )
              .map(_.collect {
                case HalfEdge(edgeType, dir, other)
                    if edgeTypes.forall(_.contains(edgeType)) &&
                      dir != EdgeDirection.Undirected &&
                      !seen.contains(other) && !toExpand.contains(other) =>
                  val rel = dir match {
                    case EdgeDirection.Outgoing =>
                      Expr.Relationship(qid, edgeType, Map.empty, other)
                    case EdgeDirection.Incoming =>
                      Expr.Relationship(other, edgeType, Map.empty, qid)
                    case EdgeDirection.Undirected =>
                      throw new IllegalStateException("this should be unreachable")
                  }
                  other -> ((qid, rel) :: path)
              })
          }
          .map(
            _.foldLeft(Map.newBuilder[QuineId, List[(QuineId, Expr.Relationship)]])(_ ++= _)
              .result()
          )

      /** Essentially, this does two breadth-first searches (via stepOutwards), alternating which
        * side is searching, until either the max length is surpassed or the two searches have a
        * common node (in which case there is a path)
        *
        * @param seenFromStart all nodes seen from the start
        * @param progressFromStart closest nodes from the start
        * @param seenFromEnd all nodes seen from the end
        * @param progressFromEnd closest nodes from the end
        * @param forward is the "start" the actual start (or are the swapped)
        * @param currentPathLength total number of steps taken from either extremity
        */
      def bidirectionalSearch(
        seenFromStart: Set[QuineId],
        progressFromStart: Map[QuineId, List[(QuineId, Expr.Relationship)]],
        seenFromEnd: Set[QuineId],
        progressFromEnd: Map[QuineId, List[(QuineId, Expr.Relationship)]],
        forward: Boolean = true,
        currentPathLength: Int = 0
      ): Future[Option[Expr.Path]] = {

        // Give up if we exceed the path limit
        if (currentPathLength > maxLength)
          return Future.successful(None)

        /* This check ensures that `progressFromEnd` is the larger of the two maps.
         * Reason: we want to take a step starting from the side that has seen the fewest nodes
         */
        if (progressFromStart.size > progressFromEnd.size)
          return bidirectionalSearch(
            seenFromEnd,
            progressFromEnd,
            seenFromStart,
            progressFromStart,
            !forward,
            currentPathLength
          )

        // Look to see if we have found a path and are done
        val shortestPathResults = progressFromStart.iterator
          .collect {
            case (key, p1) if progressFromEnd.contains(key) =>
              val p2 = progressFromEnd(key)
              if (forward) {
                (p1.reverse, key, p2.map { case (k, r) => r -> k })
              } else {
                (p2.reverse, key, p1.map { case (k, r) => r -> k })
              }
          }
          .filter {
            case (Nil, _, Nil) => allowEmpty
            case _ => true
          }
          .map { case (startToMiddle, middle, middleToEnd) =>
            // Turn the path back into the canonical format...
            val (headPath, restPath) = startToMiddle match {
              case Nil => (middle, middleToEnd.toVector)
              case (headNode, headRel) :: restToMiddle =>
                val relsToMiddle = headRel +: restToMiddle.map(_._2)
                val nodesToMiddle = restToMiddle.map(_._1) :+ middle
                val rest = (relsToMiddle zip nodesToMiddle).toVector ++ middleToEnd.toVector
                (headNode, rest)
            }

            // Fetch out all of the properties/labels of the nodes on the path
            for {
              headPathNode <- getNode(headPath, graph, atTime)
              restPathNodes <- Future.traverse(restPath) { case (rel, qid) =>
                getNode(qid, graph, atTime).map(rel -> _)
              }
            } yield Expr.Path(headPathNode, restPathNodes)
          }

        // Return the results - a single path
        if (shortestPathResults.hasNext)
          shortestPathResults.next().map(Some(_))
        else
          // by this point, we know we don't yet have a shortest path
          for {
            newProgressFromStart <- stepOutwards(
              seenFromStart,
              progressFromStart,
              if (forward) directionFilter else directionFilter.map(_.reverse)
            )
            newSeenFromStart = seenFromStart | progressFromStart.keySet
            result <- bidirectionalSearch(
              seenFromEnd,
              progressFromEnd,
              newSeenFromStart,
              newProgressFromStart,
              !forward,
              currentPathLength + 1
            )
          } yield result
      }

      Source
        .lazyFutureSource { () =>
          val pathOptFut: Future[Option[Expr.Path]] = bidirectionalSearch(
            seenFromStart = Set.empty[QuineId],
            progressFromStart = Map(startNode -> Nil),
            seenFromEnd = Set.empty[QuineId],
            progressFromEnd = Map(endNode -> Nil)
          )
          pathOptFut.map {
            case Some(path) => Source.single(Vector(path))
            case _ => Source.empty
          }
        }
    }

  }

  final case class UserDefined(name: String) extends Proc {
    private lazy val underlying = userDefinedProcedures(name.toLowerCase)

    def outputColumns = underlying.outputColumns
    def canContainUpdates: Boolean = underlying.canContainUpdates
    def canContainAllNodeScan: Boolean = underlying.canContainAllNodeScan

    def isIdempotent: Boolean = underlying.isIdempotent

    def call(
      context: QueryContext,
      arguments: Seq[Value],
      location: ProcedureExecutionLocation
    )(implicit
      ec: ExecutionContext,
      parameters: Parameters,
      timeout: Timeout
    ): Source[Vector[Value], _] =
      underlying.call(context, arguments, location)
  }
}
