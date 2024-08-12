package com.thatdot.quine.compiler.cypher

import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.UUID
import java.util.concurrent.TimeoutException

import scala.annotation.nowarn
import scala.collection.concurrent
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationLong

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import cats.syntax.either._
import io.circe.parser.parse
import org.opencypher.v9_0.ast
import org.opencypher.v9_0.frontend.phases._
import org.opencypher.v9_0.util.StepSequencer.Condition
import org.opencypher.v9_0.util.{InputPosition, Rewriter, bottomUp}

import com.thatdot.quine.compiler.cypher
import com.thatdot.quine.graph.cypher.Expr.toQuineValue
import com.thatdot.quine.graph.cypher.{
  CypherException,
  Expr,
  Func,
  Parameters,
  Proc,
  ProcedureExecutionLocation,
  QueryContext,
  Type,
  UserDefinedFunction,
  UserDefinedFunctionSignature,
  UserDefinedProcedure,
  UserDefinedProcedureSignature,
  Value
}
import com.thatdot.quine.graph.messaging.LiteralMessage._
import com.thatdot.quine.graph.{
  AlgorithmGraph,
  LiteralOpsGraph,
  NamespaceId,
  StandingQueryId,
  StandingQueryOpsGraph,
  StandingQueryResult
}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, PropertyValue, QuineId, QuineIdProvider, QuineValue}
import com.thatdot.quine.util.Log._

/** Like [[UnresolvedCall]] but where the procedure has been resolved
  *
  * @param resolvedProcedure the procedure that will get called
  * @param unresolvedCall the original call (contains arguments, returns, etc.)
  * INV: All [[ast.CallClause]]s are either [[ast.UnresolvedCall]] or [[QuineProcedureCall]], and in a fully
  * compiled query, all [[ast.CallClause]] are [[QuineProcedureCall]]
  */
final case class QuineProcedureCall(
  resolvedProcedure: UserDefinedProcedure,
  unresolvedCall: ast.UnresolvedCall
) extends ast.CallClause {

  override def clauseSpecificSemanticCheck = unresolvedCall.semanticCheck

  override def yieldAll: Boolean = true

  override def returnColumns = unresolvedCall.returnColumns

  override def containsNoUpdates: Boolean = !resolvedProcedure.canContainUpdates

  override val position: InputPosition = unresolvedCall.position
}

/** Re-write unresolved calls into variants that are resolved according to a
  * global map of UDPs
  */
case object resolveCalls extends StatementRewriter {

  /** Procedures known at Quine compile-time
    * NB some of these are only stubs -- see [[StubbedUserDefinedProcedure]]
    */
  val builtInProcedures: List[UserDefinedProcedure] = List(
    CypherIndexes,
    CypherRelationshipTypes,
    CypherFunctions,
    CypherProcedures,
    CypherPropertyKeys,
    CypherLabels,
    CypherDoWhen,
    CypherDoIt,
    CypherDoCase,
    CypherRunTimeboxed,
    CypherSleep,
    CypherCreateRelationship,
    CypherCreateSetProperty,
    CypherCreateSetLabels,
    RecentNodes,
    RecentNodeIds,
    JsonLoad,
    IncrementCounter, // TODO remove on breaking change
    AddToInt,
    AddToFloat,
    InsertToSet,
    UnionToSet,
    CypherLogging,
    CypherDebugNode,
    CypherGetDistinctIDSqSubscriberResults,
    CypherGetDistinctIdSqSubscriptionResults,
    PurgeNode,
    CypherDebugSleep,
    ReifyTime,
    RandomWalk
  )

  /** This map is only meant to maintain backward compatibility for a short time. */
  val deprecatedNames: Map[String, UserDefinedProcedure] = Map()

  private val procedures: concurrent.Map[String, UserDefinedProcedure] = Proc.userDefinedProcedures
  builtInProcedures.foreach(registerUserDefinedProcedure)
  procedures ++= deprecatedNames.map { case (rename, p) => rename.toLowerCase -> p }

  val rewriteCall: PartialFunction[AnyRef, AnyRef] = { case uc: ast.UnresolvedCall =>
    val ucName = (uc.procedureNamespace.parts :+ uc.procedureName.name).mkString(".")
    procedures.get(ucName.toLowerCase) match {
      case None => uc
      case Some(proc) => QuineProcedureCall(proc, uc)
    }
  }

  override def instance(bs: BaseState, ctx: BaseContext): Rewriter = bottomUp(Rewriter.lift(rewriteCall))

  // TODO: add to this
  override def postConditions: Set[Condition] = Set.empty
}

/** Get recently touched node IDs from shards */
object RecentNodeIds extends UserDefinedProcedure {
  val name = "recentNodeIds"
  val canContainUpdates = false
  val isIdempotent = false
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("count" -> Type.Integer),
    outputs = Vector("nodeId" -> Type.Anything),
    description = "Fetch the specified number of IDs of nodes from the in-memory cache"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], _] = {
    val limit: Int = arguments match {
      case Seq() => 10
      case Seq(Expr.Integer(l)) => l.toInt
      case other => throw wrongSignature(other)
    }

    Source.lazyFutureSource { () =>
      location.graph
        .recentNodes(limit, location.namespace, location.atTime)
        .map { (nodes: Set[QuineId]) =>
          Source(nodes)
            .map(qid => Vector(Expr.Str(qid.pretty(location.idProvider))))
        }(location.graph.nodeDispatcherEC)
    }
  }
}

/** Get recently touched nodes from shards */
object RecentNodes extends UserDefinedProcedure {
  val name = "recentNodes"
  val canContainUpdates = false
  val isIdempotent = false
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("count" -> Type.Integer),
    outputs = Vector("node" -> Type.Node),
    description = "Fetch the specified number of nodes from the in-memory cache"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], _] = {
    val limit: Int = arguments match {
      case Seq() => 10
      case Seq(Expr.Integer(l)) => l.toInt
      case other => throw wrongSignature(other)
    }
    val atTime = location.atTime
    val graph = LiteralOpsGraph.getOrThrow(s"`$name` procedure", location.graph)

    Source.lazyFutureSource { () =>
      graph
        .recentNodes(limit, location.namespace, atTime)
        .map { (nodes: Set[QuineId]) =>
          Source(nodes)
            .mapAsync(parallelism = 1)(UserDefinedProcedure.getAsCypherNode(_, location.namespace, atTime, graph))
            .map(Vector(_))
        }(location.graph.nodeDispatcherEC)
    }
  }
}

// This is required to be defined for `cypher-shell` version 4.0 and above to work
final case class CypherGetRoutingTable(addresses: Seq[String]) extends UserDefinedProcedure {
  val name = "dbms.cluster.routing.getRoutingTable"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("context" -> Type.Map, "database" -> Type.Str),
    outputs = Vector("ttl" -> Type.Integer, "servers" -> Type.List(Type.Str)),
    description = ""
  )

  // TODO: use the argument(s)
  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] =
    Source.single(
      Vector(
        Expr.Integer(-1L),
        Expr.List(Vector("WRITE", "READ", "ROUTE").map { role =>
          Expr.Map(
            Map(
              "addresses" -> Expr.List(addresses.map(Expr.Str(_)).toVector),
              "role" -> Expr.Str(role)
            )
          )
        })
      )
    )
}

object JsonLoad extends UserDefinedProcedure {
  val name = "loadJsonLines"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("url" -> Type.Str),
    outputs = Vector("value" -> Type.Anything),
    description = "Load a line-base JSON file, emitting one record per line"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], _] = {
    val urlOrPath = arguments match {
      case Seq(Expr.Str(s)) => s
      case other => throw wrongSignature(other)
    }

    Source.fromIterator(() =>
      scala.io.Source
        .fromURL(urlOrPath)
        .getLines()
        .map((line: String) => Vector(Value.fromJson(parse(line).valueOr(throw _))))
    )
  }
}

/** Procedures which are not currently implemented in Quine, but which must be present
  * for certain external systems to operate with Quine (eg cypher-shell or neo4j-browser)
  */
abstract class StubbedUserDefinedProcedure(
  override val name: String,
  outputColumnNames: Vector[String]
) extends UserDefinedProcedure {
  // Stubbed procedures are used for compatibility with other systems, therefore we avoid any Quine-specific semantic analysis
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector.empty,
    outputs = outputColumnNames.map(_ -> Type.Anything),
    description = ""
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], _] = Source.empty
}

object CypherIndexes
    extends StubbedUserDefinedProcedure(
      name = "db.indexes",
      outputColumnNames = Vector(
        "description",
        "indexName",
        "tokenNames",
        "properties",
        "state",
        "type",
        "progress",
        "provider",
        "id",
        "failureMessage"
      )
    )

object CypherRelationshipTypes
    extends StubbedUserDefinedProcedure(
      name = "db.relationshipTypes",
      outputColumnNames = Vector("relationshipType")
    )

object CypherPropertyKeys
    extends StubbedUserDefinedProcedure(
      name = "db.propertyKeys",
      outputColumnNames = Vector("propertyKey")
    )

object CypherLabels
    extends StubbedUserDefinedProcedure(
      name = "dbms.labels",
      outputColumnNames = Vector("label")
    )

/** Increment an integer property on a node atomically (doing the get and the
  * set in one step with no intervening operation)
  *
  * TODO remove at next breaking change
  */
object IncrementCounter extends UserDefinedProcedure {
  val name = "incrementCounter"
  val canContainUpdates = true
  val isIdempotent = false
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("node" -> Type.Node, "key" -> Type.Str, "amount" -> Type.Integer),
    outputs = Vector("count" -> Type.Integer),
    description =
      "Atomically increment an integer property on a node by a certain amount, returning the resultant value"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {
    import location._

    // Pull out the arguments
    val (nodeId, propertyKey, incrementQuantity) = arguments match {
      case Seq(Expr.Node(id, _, _), Expr.Str(key)) => (id, key, 1L)
      case Seq(Expr.Node(id, _, _), Expr.Str(key), Expr.Integer(amount)) => (id, key, amount)
      case other => throw wrongSignature(other)
    }

    Source.lazyFuture { () =>
      nodeId
        .?(IncrementProperty(Symbol(propertyKey), incrementQuantity, _): @nowarn)
        .map {
          case IncrementProperty.Success(newCount) => Vector(Expr.Integer(newCount))
          case IncrementProperty.Failed(valueFound) =>
            throw CypherException.TypeMismatch(
              expected = Seq(Type.Integer),
              actualValue = Expr.fromQuineValue(valueFound),
              context = "`incrementCounter` procedure"
            )
        }(location.graph.nodeDispatcherEC)
    }
  }
}

/** Increment an integer property on a node atomically (doing the get and the
  * set in one step with no intervening operation)
  */
object AddToInt extends UserDefinedProcedure with LazySafeLogging {
  val name = "int.add"
  val canContainUpdates = true
  val isIdempotent = false
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("node" -> Type.Node, "key" -> Type.Str, "add" -> Type.Integer),
    outputs = Vector("result" -> Type.Integer),
    description = """Atomically add to an integer property on a node by a certain amount (defaults to 1),
                    |returning the resultant value""".stripMargin.replace('\n', ' ')
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {
    import location._

    val nodeId = arguments.headOption
      .flatMap(UserDefinedProcedure.extractQuineId)
      .getOrElse(throw CypherException.Runtime(s"`$name` expects a node or node ID as its first argument"))
    // Pull out the arguments
    val (propertyKey, incrementQuantity) = arguments match {
      case Seq(_, Expr.Str(key)) => (key, 1L)
      case Seq(_, Expr.Str(key), Expr.Integer(amount)) => (key, amount)
      case other => throw wrongSignature(other)
    }

    Source.lazyFuture { () =>
      nodeId
        .?(AddToAtomic.Int(Symbol(propertyKey), QuineValue.Integer(incrementQuantity), _))
        .map {
          case AddToAtomicResult.SuccessInt(newCount) => Vector(Expr.fromQuineValue(newCount))
          case AddToAtomicResult.Failed(valueFound) =>
            throw CypherException.TypeMismatch(
              expected = Seq(Type.Integer),
              actualValue = Expr.fromQuineValue(valueFound),
              context = s"Property accessed by $name procedure"
            )
          case successOfDifferentType: AddToAtomicResult =>
            // by the type invariant on [[AddToAtomic]], this case is unreachable.
            logger.warn(
              log"""Verify data integrity on node: ${Safe(nodeId.pretty)}. Property: ${Safe(propertyKey)}
                   |reports a current value of ${successOfDifferentType.valueFound.toString} but reports
                   |successfully being updated as an integer by: ${Safe(name)}.""".cleanLines
            )
            throw CypherException.TypeMismatch(
              expected = Seq(Type.Integer),
              actualValue = Expr.fromQuineValue(successOfDifferentType.valueFound),
              context = s"Property accessed by $name procedure."
            )
        }(location.graph.nodeDispatcherEC)
    }
  }
}

/** Increment a floating-point property on a node atomically (doing the get and the
  * set in one step with no intervening operation)
  */
object AddToFloat extends UserDefinedProcedure with LazySafeLogging {
  val name = "float.add"
  val canContainUpdates = true
  val isIdempotent = false
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("node" -> Type.Node, "key" -> Type.Str, "add" -> Type.Floating),
    outputs = Vector("result" -> Type.Floating),
    description = """Atomically add to a floating-point property on a node by a certain amount (defaults to 1.0),
                    |returning the resultant value""".stripMargin.replace('\n', ' ')
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {
    import location._

    val nodeId = arguments.headOption
      .flatMap(UserDefinedProcedure.extractQuineId)
      .getOrElse(throw CypherException.Runtime(s"`$name` expects a node or node ID as its first argument"))
    // Pull out the arguments
    val (propertyKey, incrementQuantity) = arguments match {
      case Seq(_, Expr.Str(key)) => (key, 1.0)
      case Seq(_, Expr.Str(key), Expr.Floating(amount)) => (key, amount)
      case other => throw wrongSignature(other)
    }

    Source.lazyFuture { () =>
      nodeId
        .?(AddToAtomic.Float(Symbol(propertyKey), QuineValue.Floating(incrementQuantity), _))
        .map {
          case AddToAtomicResult.SuccessFloat(newCount) => Vector(Expr.fromQuineValue(newCount))
          case AddToAtomicResult.Failed(valueFound) =>
            throw CypherException.TypeMismatch(
              expected = Seq(Type.Floating),
              actualValue = Expr.fromQuineValue(valueFound),
              context = s"Property accessed by $name procedure"
            )
          case successOfDifferentType: AddToAtomicResult =>
            // by the type invariant on [[AddToAtomic]], this case is unreachable.
            logger.warn(
              log"""Verify data integrity on node: ${Safe(nodeId.pretty)}. Property: ${Safe(propertyKey)} reports a current value
                   |of ${successOfDifferentType.valueFound.toString} but reports successfully being updated as a float
                   |by: ${Safe(name)}.""".cleanLines
            )
            throw CypherException.TypeMismatch(
              expected = Seq(Type.Floating),
              actualValue = Expr.fromQuineValue(successOfDifferentType.valueFound),
              context = s"Property accessed by $name procedure."
            )
        }(location.graph.nodeDispatcherEC)
    }
  }
}

/** Add to a list-typed property on a node atomically, treating the list as a set (doing the get, the deduplication, and
  * the set in one step with no intervening operation)
  */
object InsertToSet extends UserDefinedProcedure with LazySafeLogging {
  val name = "set.insert"
  val canContainUpdates = true
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("node" -> Type.Node, "key" -> Type.Str, "add" -> Type.Anything),
    outputs = Vector("result" -> Type.ListOfAnything),
    description =
      """Atomically add an element to a list property treated as a set. If one or more instances of `add` are
        |already present in the list at node[key], this procedure has no effect.""".stripMargin.replace('\n', ' ')
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {
    import location._

    val nodeId = arguments.headOption
      .flatMap(UserDefinedProcedure.extractQuineId)
      .getOrElse(throw CypherException.Runtime(s"`$name` expects a node or node ID as its first argument"))
    // Pull out the arguments
    val (propertyKey, newElements) = arguments match {
      case Seq(_, Expr.Str(key), elem) => (key, QuineValue.List(Vector(Expr.toQuineValue(elem))))
      case other => throw wrongSignature(other)
    }

    Source.lazyFuture { () =>
      nodeId
        .?(AddToAtomic.Set(Symbol(propertyKey), newElements, _))
        .map {
          case AddToAtomicResult.SuccessList(newCount) => Vector(Expr.fromQuineValue(newCount))
          case AddToAtomicResult.Failed(valueFound) =>
            throw CypherException.TypeMismatch(
              expected = Seq(Type.ListOfAnything),
              actualValue = Expr.fromQuineValue(valueFound),
              context = s"Property accessed by $name procedure"
            )
          case successOfDifferentType: AddToAtomicResult =>
            // by the type invariant on [[AddToAtomic]], this case is unreachable.
            logger.warn(
              log"""Verify data integrity on node: ${Safe(nodeId.pretty)}. Property: ${Safe(propertyKey)}
                   |reports a current value of ${successOfDifferentType.valueFound.toString} but reports
                   |successfully being updated as a list (used as set) by: ${Safe(name)}.""".cleanLines
            )
            throw CypherException.TypeMismatch(
              expected = Seq(Type.ListOfAnything),
              actualValue = Expr.fromQuineValue(successOfDifferentType.valueFound),
              context = s"Property accessed by $name procedure."
            )
        }(location.graph.nodeDispatcherEC)
    }
  }
}

/** Add to a list-typed property on a node atomically, treating the list as a set (doing the get, the deduplication, and
  * the set in one step with no intervening operation)
  */
object UnionToSet extends UserDefinedProcedure with LazySafeLogging {
  val name = "set.union"
  val canContainUpdates = true
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("node" -> Type.Node, "key" -> Type.Str, "add" -> Type.ListOfAnything),
    outputs = Vector("result" -> Type.ListOfAnything),
    description =
      """Atomically add set of elements to a list property treated as a set. The elements in `add` will be deduplicated
        |and, for any that are not yet present at node[key], will be stored. If the list at node[key] already contains
        |all elements of `add`, this procedure has no effect.""".stripMargin.replace('\n', ' ')
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {
    import location._

    val nodeId = arguments.headOption
      .flatMap(UserDefinedProcedure.extractQuineId)
      .getOrElse(throw CypherException.Runtime(s"`$name` expects a node or node ID as its first argument"))
    // Pull out the arguments
    val (propertyKey, newElements) = arguments match {
      case Seq(_, Expr.Str(key), Expr.List(cypherElems)) => (key, QuineValue.List(cypherElems.map(Expr.toQuineValue)))
      case other => throw wrongSignature(other)
    }

    Source.lazyFuture { () =>
      nodeId
        .?(AddToAtomic.Set(Symbol(propertyKey), newElements, _))
        .map {
          case AddToAtomicResult.SuccessList(newCount) => Vector(Expr.fromQuineValue(newCount))
          case AddToAtomicResult.Failed(valueFound) =>
            throw CypherException.TypeMismatch(
              expected = Seq(Type.ListOfAnything),
              actualValue = Expr.fromQuineValue(valueFound),
              context = s"Property accessed by $name procedure"
            )
          case successOfDifferentType: AddToAtomicResult =>
            // by the type invariant on [[AddToAtomic]], this case is unreachable.
            logger.warn(
              log"""Verify data integrity on node: ${Safe(nodeId.pretty)}. Property: ${Safe(propertyKey)} reports a
                   |current value of ${successOfDifferentType.valueFound.toString} but reports successfully being
                   |updated as a list (used as set) by: ${Safe(name)}.""".cleanLines
            )
            throw CypherException.TypeMismatch(
              expected = Seq(Type.ListOfAnything),
              actualValue = Expr.fromQuineValue(successOfDifferentType.valueFound),
              context = s"Property accessed by $name procedure."
            )
        }(location.graph.nodeDispatcherEC)
    }
  }
}

object CypherLogging extends UserDefinedProcedure with StrictSafeLogging {
  val name = "log"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("level" -> Type.Str, "value" -> Type.Anything),
    outputs = Vector("log" -> Type.Str),
    description = "Log the input argument to console"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {

    val prettyStr: String = arguments match {
      case Seq(Expr.Str(lvl), any) =>
        val prettied = any.pretty
        val sprettied = Safe(prettied)
        lvl.toLowerCase match {
          case "error" => logger.error(safe"$sprettied")
          case "warn" | "warning" => logger.warn(safe"$sprettied")
          case "info" => logger.info(safe"$sprettied")
          case "debug" => logger.debug(safe"$sprettied")
          case "trace" => logger.trace(safe"$sprettied")
          case other =>
            logger.error(safe"Unrecognized log level ${Safe(other)}, falling back to `warn`")
            logger.warn(safe"$sprettied")
        }
        prettied

      case Seq(any) =>
        val prettied = any.pretty
        logger.warn(safe"${Safe(prettied)}")
        prettied

      case other => throw wrongSignature(other)
    }

    Source.single(Vector(Expr.Str(prettyStr)))
  }
}

object CypherDebugNode extends UserDefinedProcedure {
  val name = "debug.node"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("node" -> Type.Anything),
    outputs = Vector(
      "atTime" -> Type.LocalDateTime,
      "properties" -> Type.Map,
      "edges" -> Type.ListOfAnything,
      "latestUpdateMillisAfterSnapshot" -> Type.Integer,
      "subscribers" -> Type.Str,
      "subscriptions" -> Type.Str,
      "multipleValuesStandingQueryStates" -> Type.ListOfAnything,
      "journal" -> Type.ListOfAnything,
      "graphNodeHashCode" -> Type.Integer
    ),
    description = "Log the internal state of a node"
  )

  private[this] def halfEdge2Value(edge: HalfEdge)(implicit idProvider: QuineIdProvider): Value =
    Expr.Map(
      Map(
        "edgeType" -> Expr.Str(edge.edgeType.name),
        "direction" -> Expr.Str(edge.direction.toString),
        "other" -> Expr.fromQuineValue(idProvider.qidToValue(edge.other))
      )
    )

  private[this] def locallyRegisteredStandingQuery2Value(q: LocallyRegisteredStandingQuery): Value =
    Expr.Map(
      Map(
        "id" -> Expr.Str(q.id),
        "globalId" -> Expr.Str(q.globalId),
        "subscribers" -> Expr.List(q.subscribers.view.map(Expr.Str).toVector),
        "state" -> Expr.Str(q.state)
      )
    )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {

    val graph = LiteralOpsGraph.getOrThrow(s"`$name` procedure", location.graph)
    implicit val idProv: QuineIdProvider = graph.idProvider

    val node: QuineId = arguments match {
      case Seq(nodeLike) =>
        UserDefinedProcedure.extractQuineId(nodeLike) getOrElse (throw CypherException.Runtime(
          s"`$name` expects a node or node ID argument, but got $nodeLike"
        ))
      case other =>
        throw wrongSignature(other)
    }

    Source.lazyFuture { () =>
      graph
        .literalOps(location.namespace)
        .logState(node, location.atTime)
        .map {
          case NodeInternalState(
                atTime,
                properties,
                edges,
                latestUpdateMillisAfterSnapshot,
                subscribers,
                subscriptions,
                _,
                _,
                multipleValuesStandingQueryStates,
                journal,
                graphNodeHashCode
              ) =>
            Vector(
              atTime
                .map(t =>
                  Expr.DateTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(t.millis), ZoneId.systemDefault()))
                )
                .getOrElse(Expr.Null),
              Expr.Map(properties.map(kv => kv._1.name -> Expr.Str(kv._2))),
              Expr.List(edges.view.map(halfEdge2Value).toVector),
              latestUpdateMillisAfterSnapshot match {
                case None => Expr.Null
                case Some(eventTime) => Expr.Integer(eventTime.millis)
              },
              Expr.Str(subscribers.mkString(",")),
              Expr.Str(subscriptions.mkString(",")),
              Expr.List(multipleValuesStandingQueryStates.map(locallyRegisteredStandingQuery2Value)),
              Expr.List(journal.map(e => Expr.Str(e.toString)).toVector),
              Expr.Integer(graphNodeHashCode)
            )
        }(location.graph.nodeDispatcherEC)
    }
  }
}

object CypherGetDistinctIDSqSubscriberResults extends UserDefinedProcedure {
  def name: String = "subscribers"
  def canContainUpdates: Boolean = false
  def isIdempotent: Boolean = true
  def canContainAllNodeScan: Boolean = false

  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("node" -> Type.Anything),
    outputs = Vector(
      "queryId" -> Type.Integer,
      "queryDepth" -> Type.Integer,
      "receiverId" -> Type.Str,
      "lastResult" -> Type.Anything
    ),
    description = "Return the current state of the standing query subscribers."
  )

  def call(context: QueryContext, arguments: Seq[Value], location: ProcedureExecutionLocation)(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], _] = {
    val graph: LiteralOpsGraph = LiteralOpsGraph.getOrThrow(s"`$name` procedure", location.graph)
    implicit val idProv: QuineIdProvider = location.graph.idProvider

    val node: QuineId = arguments match {
      case Seq(nodeLike) =>
        UserDefinedProcedure.extractQuineId(nodeLike) getOrElse (throw CypherException.Runtime(
          s"`$name` expects a node or node ID argument, but got $nodeLike"
        ))
      case other =>
        throw wrongSignature(other)
    }

    Source.lazyFutureSource { () =>
      graph
        .literalOps(location.namespace)
        .getSqResults(node)
        .map(sqr =>
          Source.fromIterator { () =>
            sqr.subscribers.map { s =>
              Vector(
                Expr.Integer(s.dgnId),
                Expr.Str(s.qid.pretty),
                s.lastResult.fold[Value](Expr.Null)(r => Expr.Bool(r))
              )
            }.iterator
          }
        )(location.graph.nodeDispatcherEC)
    }
  }
}

object CypherGetDistinctIdSqSubscriptionResults extends UserDefinedProcedure {
  def name: String = "subscriptions"
  def canContainUpdates: Boolean = false
  def isIdempotent: Boolean = true
  def canContainAllNodeScan: Boolean = false

  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("node" -> Type.Anything),
    outputs = Vector(
      "queryId" -> Type.Integer,
      "queryDepth" -> Type.Integer,
      "receiverId" -> Type.Str,
      "lastResult" -> Type.Anything
    ),
    description = "Return the current state of the standing query subscriptions."
  )

  def call(context: QueryContext, arguments: Seq[Value], location: ProcedureExecutionLocation)(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], _] = {
    val graph: LiteralOpsGraph = LiteralOpsGraph.getOrThrow(s"`$name` procedure", location.graph)
    implicit val idProv: QuineIdProvider = location.graph.idProvider

    val node: QuineId = arguments match {
      case Seq(nodeLike) =>
        UserDefinedProcedure.extractQuineId(nodeLike) getOrElse (throw CypherException.Runtime(
          s"`$name` expects a node or node ID argument, but got $nodeLike"
        ))
      case other =>
        throw wrongSignature(other)
    }

    Source.lazyFutureSource { () =>
      graph
        .literalOps(location.namespace)
        .getSqResults(node)
        .map(sqr =>
          Source.fromIterator { () =>
            sqr.subscriptions.map { s =>
              Vector(
                Expr.Integer(s.dgnId.toLong),
                Expr.Str(s.qid.pretty),
                s.lastResult.fold[Value](Expr.Null)(r => Expr.Bool(r))
              )
            }.iterator
          }
        )(location.graph.nodeDispatcherEC)
    }
  }
}

object PurgeNode extends UserDefinedProcedure {
  val name = "purgeNode"
  val canContainUpdates = true
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("node" -> Type.Anything),
    outputs = Vector.empty,
    description = "Purge a node from history"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {

    val graph = LiteralOpsGraph.getOrThrow(s"$name Cypher procedure", location.graph)
    implicit val idProv: QuineIdProvider = graph.idProvider

    val node: QuineId = arguments match {
      case Seq(nodeLike) =>
        UserDefinedProcedure.extractQuineId(nodeLike) getOrElse (throw CypherException.Runtime(
          s"`$name` expects a node or node ID argument, but got $nodeLike"
        ))
      case other =>
        throw wrongSignature(other)
    }

    Source.lazyFuture { () =>
      graph.literalOps(location.namespace).purgeNode(node).map(_ => Vector.empty)(ExecutionContext.parasitic)
    }
  }
}
object CypherDebugSleep extends UserDefinedProcedure {
  val name = "debug.sleep"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("node" -> Type.Anything),
    outputs = Vector.empty,
    description = "Request a node sleep"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {

    val graph = location.graph
    implicit val idProv: QuineIdProvider = graph.idProvider

    val node: QuineId = arguments match {
      case Seq(nodeLike) =>
        UserDefinedProcedure.extractQuineId(nodeLike) getOrElse (throw CypherException.Runtime(
          s"`$name` expects a node or node ID argument, but got $nodeLike"
        ))
      case other =>
        throw wrongSignature(other)
    }

    Source.lazyFuture { () =>
      graph.requestNodeSleep(location.namespace, node).map(_ => Vector.empty)(location.graph.nodeDispatcherEC)
    }
  }
}

object CypherBuiltinFunctions extends UserDefinedProcedure {
  val name = "help.builtins"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector.empty,
    outputs = Vector("name" -> Type.Str, "signature" -> Type.Str, "description" -> Type.Str),
    description = "List built-in cypher functions"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {

    arguments match {
      case Seq() =>
      case other => throw wrongSignature(other)
    }

    Source
      .fromIterator(() => Func.builtinFunctions.sortBy(_.name).iterator)
      .map(bfc => Vector(Expr.Str(bfc.name), Expr.Str(bfc.signature), Expr.Str(bfc.description)))
  }
}

object CypherFunctions extends UserDefinedProcedure {
  val name = "help.functions"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector.empty,
    outputs = Vector("name" -> Type.Str, "signature" -> Type.Str, "description" -> Type.Str),
    description = "List registered functions"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {

    arguments match {
      case Seq() =>
      case other => throw wrongSignature(other)
    }

    val builtins =
      Func.builtinFunctions
        .sortBy(_.name)
        .map(bfc => Vector(Expr.Str(bfc.name), Expr.Str(bfc.signature), Expr.Str(bfc.description)))

    val userDefined =
      Func.userDefinedFunctions.values.toList
        .sortBy(_.name)
        .flatMap { (udf: UserDefinedFunction) =>
          val name = udf.name
          udf.signatures.toVector.map { (udfSig: UserDefinedFunctionSignature) =>
            Vector(Expr.Str(name), Expr.Str(udfSig.pretty(name)), Expr.Str(udfSig.description))
          }
        }

    Source((builtins ++ userDefined).sortBy(_.head.string))
  }
}

object CypherProcedures extends UserDefinedProcedure {
  val name = "help.procedures"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector.empty,
    outputs = Vector(
      "name" -> Type.Str,
      "signature" -> Type.Str,
      "description" -> Type.Str,
      "mode" -> Type.Str
    ),
    description = "List registered procedures"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {

    arguments match {
      case Seq() =>
      case other => throw wrongSignature(other)
    }

    Source
      .fromIterator(() => Proc.userDefinedProcedures.values.toList.sortBy(_.name).iterator)
      .map { (udp: UserDefinedProcedure) =>
        val name = udp.name
        val sig = udp.signature.pretty(udp.name)
        val description = udp.signature.description
        val mode = if (udp.canContainUpdates) "WRITE" else "READ"
        Vector(Expr.Str(name), Expr.Str(sig), Expr.Str(description), Expr.Str(mode))
      }
  }
}

object CypherDoWhen extends UserDefinedProcedure {
  val name = "do.when"
  val canContainUpdates = true
  val isIdempotent = false
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector(
      "condition" -> Type.Bool,
      "ifQuery" -> Type.Str,
      "elseQuery" -> Type.Str,
      "params" -> Type.Map
    ),
    outputs = Vector("value" -> Type.Map),
    description = "Depending on the condition execute ifQuery or elseQuery"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {

    // This helper function is to work around an (possibly compiler) error for the subsequent
    // `val`. If you try to inline the pattern match with the `val`, you'll get a warning
    // from the exhaustiveness checker.
    def extractSeq(values: Seq[Value]): (Boolean, String, String, Map[String, Value]) = values match {
      case Seq(Expr.Bool(c), Expr.Str(ifQ)) => (c, ifQ, "", Map.empty)
      case Seq(Expr.Bool(c), Expr.Str(ifQ), Expr.Str(elseQ)) => (c, ifQ, elseQ, Map.empty)
      case Seq(Expr.Bool(c), Expr.Str(ifQ), Expr.Str(elseQ), Expr.Map(p)) => (c, ifQ, elseQ, p)
      case other => throw wrongSignature(other)
    }

    val (cond: Boolean, ifQ: String, elseQ: String, params: Map[String, Value]) = extractSeq(arguments)

    val queryToExecute = if (cond) ifQ else elseQ

    if (queryToExecute == "") {
      Source.single(Vector(Expr.Map(Map.empty)))
    } else {
      val subQueryResults = queryCypherValues(
        queryToExecute,
        location.namespace,
        parameters = params,
        initialColumns = params,
        atTime = location.atTime
      )(
        location.graph
      )

      subQueryResults.results.map { (row: Vector[Value]) =>
        Vector(Expr.Map(subQueryResults.columns.map(_.name).zip(row.view)))
      }
    }
  }
}

object CypherDoIt extends UserDefinedProcedure {
  val name = "cypher.doIt"
  val canContainUpdates = true
  val isIdempotent = false
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("cypher" -> Type.Str, "params" -> Type.Map),
    outputs = Vector("value" -> Type.Map),
    description = "Executes a Cypher query with the given parameters"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {

    def extractSeq(values: Seq[Value]): (String, Map[String, Value]) = arguments match {
      case Seq(Expr.Str(query)) => query -> Map.empty
      case Seq(Expr.Str(query), Expr.Map(parameters)) => (query, parameters)
      case other => throw wrongSignature(other)
    }

    val (query: String, parameters: Map[String, Value]) = extractSeq(arguments)

    val subQueryResults = queryCypherValues(
      query,
      location.namespace,
      parameters = parameters,
      initialColumns = parameters,
      atTime = location.atTime
    )(
      location.graph
    )

    subQueryResults.results.map { (row: Vector[Value]) =>
      Vector(Expr.Map(subQueryResults.columns.map(_.name).zip(row.view)))
    }
  }
}

object CypherDoCase extends UserDefinedProcedure {
  val name = "cypher.do.case"
  val canContainUpdates = true
  val isIdempotent = false
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("conditionals" -> Type.ListOfAnything, "elseQuery" -> Type.Str, "params" -> Type.Map),
    outputs = Vector("value" -> Type.Map),
    description = "Given a list of conditional/query pairs, execute the first query with a true conditional"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {

    def extractSeq(values: Seq[Value]): (Vector[Value], String, Map[String, Value]) = arguments match {
      case Seq(Expr.List(conds)) => (conds, "", Map.empty)
      case Seq(Expr.List(conds), Expr.Str(els)) => (conds, els, Map.empty)
      case Seq(Expr.List(conds), Expr.Str(els), Expr.Map(params)) => (conds, els, params)
      case other => throw wrongSignature(other)
    }

    val (conditionals: Vector[Value], elseQuery: String, parameters: Map[String, Value]) = extractSeq(arguments)

    // Iterate through the conditions and queries to find the right matching query
    val matchingQuery: Option[String] = conditionals
      .grouped(2)
      .map {
        case Vector(Expr.Bool(cond), Expr.Str(query)) => cond -> query
        case Vector(_: Expr.Bool, other) =>
          throw CypherException.TypeMismatch(Seq(Type.Str), other, s"query statement in `$name`)")
        case Vector(other, _) =>
          throw CypherException.TypeMismatch(Seq(Type.Bool), other, s"condition in `$name`)")
        case _ =>
          throw CypherException.Runtime(
            s"`$name` expects each condition to be followed by a query, " +
            s"but the list of conditions and queries has odd length ${conditionals.length}"
          )
      }
      .collectFirst { case (true, query) => query }
      .orElse(Some(elseQuery))
      .filter(_ != "")

    matchingQuery match {
      case None => Source.single(Vector(Expr.Map(Map.empty)))
      case Some(query) =>
        val subQueryResults = queryCypherValues(
          query,
          location.namespace,
          parameters = parameters,
          initialColumns = parameters,
          atTime = location.atTime
        )(
          location.graph
        )

        subQueryResults.results.map { (row: Vector[Value]) =>
          Vector(Expr.Map(subQueryResults.columns.map(_.name).zip(row.view)))
        }
    }
  }
}

object CypherRunTimeboxed extends UserDefinedProcedure {
  val name = "cypher.runTimeboxed"
  val canContainUpdates = true
  val isIdempotent = false
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("cypher" -> Type.Str, "params" -> Type.Map, "timeout" -> Type.Integer),
    outputs = Vector("value" -> Type.Map),
    description = "Executes a Cypher query with the given parameters but abort after a certain number of milliseconds"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {

    val (query: String, parameters: Map[String, Value], t: Long) = arguments match {
      case Seq(Expr.Str(query), Expr.Map(parameters), Expr.Integer(t)) => (query, parameters, t)
      case other => throw wrongSignature(other)
    }

    val subQueryResults = queryCypherValues(
      query,
      location.namespace,
      parameters = parameters,
      initialColumns = parameters,
      atTime = location.atTime
    )(
      location.graph
    )

    subQueryResults.results
      .completionTimeout(t.milliseconds)
      .recoverWithRetries(1, { case _: TimeoutException => Source.empty })
      .map { (row: Vector[Value]) =>
        Vector(Expr.Map(subQueryResults.columns.map(_.name).zip(row.view)))
      }
  }
}

object CypherSleep extends UserDefinedProcedure {
  val name = "util.sleep"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("duration" -> Type.Integer),
    outputs = Vector.empty,
    description = "Sleep for a certain number of milliseconds"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {

    val sleepMillis: Long = arguments match {
      case Seq(Expr.Integer(t)) => t
      case other => throw wrongSignature(other)
    }

    Source
      .single(Vector.empty[Value])
      .initialDelay(sleepMillis.milliseconds)
  }
}

object CypherCreateRelationship extends UserDefinedProcedure {
  val name = "create.relationship"
  val canContainUpdates = true
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("from" -> Type.Node, "relType" -> Type.Str, "props" -> Type.Map, "to" -> Type.Node),
    outputs = Vector("rel" -> Type.Relationship),
    description = "Create a relationship with a potentially dynamic name"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {
    import location._

    val (from, label, to): (QuineId, Symbol, QuineId) = arguments match {
      case args @ Seq(fromNodeLike, Expr.Str(name), Expr.Map(_), toNodeLike) =>
        val from = UserDefinedProcedure.extractQuineId(fromNodeLike).getOrElse(throw wrongSignature(args))
        val to = UserDefinedProcedure.extractQuineId(toNodeLike).getOrElse(throw wrongSignature(args))
        (from, Symbol(name), to)
      case other => throw wrongSignature(other)
    }

    Source.lazyFuture { () =>
      val one = from ? (AddHalfEdgeCommand(HalfEdge(label, EdgeDirection.Outgoing, to), _))
      val two = to ? (AddHalfEdgeCommand(HalfEdge(label, EdgeDirection.Incoming, from), _))
      one.zipWith(two)((_, _) => Vector(Expr.Relationship(from, label, Map.empty, to)))(location.graph.nodeDispatcherEC)
    }
  }
}

object CypherCreateSetProperty extends UserDefinedProcedure {
  val name = "create.setProperty"
  val canContainUpdates = true
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("node" -> Type.Node, "key" -> Type.Str, "value" -> Type.Anything),
    outputs = Vector.empty,
    description = "Set the property with the provided key on the specified input node"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {
    import location._

    val (node, key, value): (QuineId, String, Value) = arguments match {
      case args @ Seq(nodeLike, Expr.Str(key), value) =>
        val node = UserDefinedProcedure.extractQuineId(nodeLike).getOrElse(throw wrongSignature(args))
        (node, key, value)
      case other => throw wrongSignature(other)
    }

    Source
      .lazyFuture(() => node ? (SetPropertyCommand(Symbol(key), PropertyValue(toQuineValue(value)), _)))
      .map(_ => Vector.empty[Value])
  }
}

object CypherCreateSetLabels extends UserDefinedProcedure {
  val name = "create.setLabels"
  val canContainUpdates = true
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("node" -> Type.Node, "labels" -> Type.List(Type.Str)),
    outputs = Vector.empty,
    description = "Set the labels on the specified input node, overriding any previously set labels"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {
    import location._

    val (node, labels): (QuineId, Set[Symbol]) = arguments match {
      case args @ Seq(fromNodeLike, Expr.List(labels)) =>
        val from = UserDefinedProcedure.extractQuineId(fromNodeLike).getOrElse(throw wrongSignature(args))
        val stringLabels = Set.newBuilder[Symbol]
        for (label <- labels)
          label match {
            case Expr.Str(l) => stringLabels += Symbol(l)
            case _ => throw wrongSignature(args)
          }
        from -> stringLabels.result()
      case other => throw wrongSignature(other)
    }

    Source
      .lazyFuture(() => node ? (SetLabels(labels, _)))
      .map(_ => Vector.empty[Value])
  }
}

/** Lookup a standing query by user-facing name, yielding its [[StandingQueryResults]] as they are produced
  * Registered by registerUserDefinedProcedure at runtime by appstate and in docs' GenerateCypherTables
  * NB despite the name including `wiretap`, this is implemented as an pekko-streams map, so it will
  * backpressure
  */
class CypherStandingWiretap(lookupByName: (String, NamespaceId) => Option[StandingQueryId])
    extends UserDefinedProcedure {
  val name = "standing.wiretap"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("options" -> Type.Map),
    outputs = Vector("data" -> Type.Map, "meta" -> Type.Map),
    description = "Wire-tap the results of a standing query"
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {

    val standingQueryId: StandingQueryId = arguments match {
      case Seq(Expr.Map(optionsMap)) =>
        val remainingOptions = scala.collection.mutable.Map(optionsMap.toSeq: _*)

        // User specified the name of the standing query
        val standingQueryName = remainingOptions.remove("name").map {
          case Expr.Str(name) => name
          case other =>
            throw CypherException.TypeMismatch(
              Seq(Type.Str),
              other,
              "`name` field in options map"
            )
        }

        // User specified the standing query ID
        val standingQueryIdStr = remainingOptions.remove("id").map {
          case Expr.Str(sqId) => sqId
          case other =>
            throw CypherException.TypeMismatch(
              Seq(Type.Str),
              other,
              "`id` field in options map"
            )
        }

        // Disallow unknown fields
        if (remainingOptions.nonEmpty) {
          throw CypherException.Runtime(
            "Unknown fields in options map: " + remainingOptions.keys.mkString("`", "`, `", "`")
          )
        }

        (standingQueryName, standingQueryIdStr) match {
          case (Some(nme), None) =>
            lookupByName(nme, location.namespace).getOrElse {
              throw CypherException.Runtime(s"Cannot find standing query with name `$nme`")
            }
          case (None, Some(strId)) =>
            try StandingQueryId(UUID.fromString(strId))
            catch {
              case _: IllegalArgumentException =>
                throw CypherException.Runtime(s"Expected standing query ID to be UUID, but got `$strId`")
            }
          case (None, None) =>
            throw CypherException.Runtime("One of `name` or `id` needs to be specified")
          case (Some(_), Some(_)) =>
            throw CypherException.Runtime("Only one of `name` or `id` needs to be specified")
        }

      case other => throw wrongSignature(other)
    }

    val graph: StandingQueryOpsGraph = StandingQueryOpsGraph(location.graph) match {
      case None =>
        val msg = s"`$name` procedure requires a graph that implements StandingQueryOperations"
        return Source.failed(new IllegalArgumentException(msg))
      case Some(g) => g
    }

    graph
      .standingQueries(location.namespace)
      .flatMap(
        _.wireTapStandingQuery(standingQueryId)
      )
      .getOrElse(throw CypherException.Runtime(s"Cannot find standing query with id `$standingQueryId`"))
      .map { case StandingQueryResult(meta, data) =>
        val dataMap = Expr.fromQuineValue(QuineValue.Map(data))
        val metaMap = Expr.fromQuineValue(QuineValue.Map(meta.toMap))
        Vector(dataMap, metaMap)
      }
  }
}

object RandomWalk extends UserDefinedProcedure {
  val name = "random.walk"
  val canContainUpdates = false
  val isIdempotent = false
  val canContainAllNodeScan = false
  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector(
      "start" -> Type.Anything,
      "depth" -> Type.Integer,
      "return" -> Type.Floating,
      "in-out" -> Type.Floating,
      "seed" -> Type.Str
    ),
    outputs = Vector("walk" -> Type.List(Type.Str)),
    description = "Randomly walk edges from a starting node for a chosen depth. " +
      "Returns a list of node IDs in the order they were encountered."
  )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig
  ): Source[Vector[Value], NotUsed] = {

    val graph = AlgorithmGraph.getOrThrow(s"`$name` procedure", location.graph)

    def toQid(nodeLike: Value): QuineId = UserDefinedProcedure
      .extractQuineId(nodeLike)(graph.idProvider) getOrElse (throw CypherException.Runtime(
      s"`$name` expects a node or node ID as the first argument, but got: $nodeLike"
    ))

    val compiledQuery = cypher.compile(AlgorithmGraph.defaults.walkQuery, unfixedParameters = List("n"))

    val (startNode, depth: Long, returnParam, inOutParam, randSeedOpt: Option[String]) = arguments match {
      case Seq(nodelike) => (toQid(nodelike), 1L, 1d, 1d, None)
      case Seq(nodelike, Expr.Integer(t)) => (toQid(nodelike), if (t >= 0) t else 0L, 1d, 1d, None)
      case Seq(nodelike, Expr.Integer(t), Expr.Floating(p)) => (toQid(nodelike), if (t >= 0) t else 0L, p, 1d, None)
      case Seq(nodelike, Expr.Integer(t), Expr.Floating(p), Expr.Floating(q)) =>
        (toQid(nodelike), if (t >= 0) t else 0L, p, q, None)
      case Seq(nodelike, Expr.Integer(t), Expr.Floating(p), Expr.Floating(q), Expr.Str(s)) =>
        (toQid(nodelike), if (t >= 0) t else 0L, p, q, Some(s))
      case other => throw wrongSignature(other)
    }

    Source.future(
      graph.algorithms
        .randomWalk(
          startNode,
          compiledQuery,
          depth.toInt,
          returnParam,
          inOutParam,
          None,
          randSeedOpt,
          location.namespace,
          location.atTime
        )
        .map { l =>
          Vector(Expr.List(l.acc.toVector.map(q => Expr.Str(q))))
        }(graph.nodeDispatcherEC)
    )
  }
}
