package com.thatdot.quine.compiler.cypher

import scala.concurrent.ExecutionContext

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.LiteralOpsGraph
import com.thatdot.quine.graph.cypher.{CypherException, Expr, UserDefinedProcedure, Value}
import com.thatdot.quine.graph.metrics.HostQuineMetrics.{HistoricalProcedureMetrics, JournalWalkMetrics}
import com.thatdot.quine.model.{EdgeDirection, Milliseconds, QuineIdProvider}

/** Shared support for the `history.*` procedures, which reconstruct the past from node journals. */
object HistoricalJournalSupport {

  /** Options a `history.*` procedure accepts, given as its last positional argument.
    *
    * Not every procedure accepts every option, so each states which it recognises and the rest are
    * rejected. The upper bound is spelled `through` rather than `until` because `until` is a
    * reserved word in Cypher — `RETURN {until: 1}` does not parse — and an option that only works
    * when backticked would be a trap. `through` also states the bound's inclusiveness plainly.
    *
    * @param since earliest millisecond to report, inclusive
    * @param through latest millisecond to report, inclusive
    * @param limit greatest number of rows to return
    * @param unreadableAsNull report a value that cannot be deserialized as null instead of failing
    */
  final case class HistoricalOptions(
    since: Option[Milliseconds] = None,
    through: Option[Milliseconds] = None,
    limit: Option[Int] = None,
    unreadableAsNull: Boolean = false,
  )

  object HistoricalOptions {
    val empty: HistoricalOptions = HistoricalOptions()

    val Since = "since"
    val Through = "through"
    val Limit = "limit"
    val UnreadableAsNull = "unreadableAsNull"

    /** Every procedure accepts at least the window and the row cap. */
    val windowKeys: Set[String] = Set(Since, Through, Limit)

    /** Read the options map, accepting only the keys the calling procedure understands.
      *
      * Unrecognised keys are rejected rather than ignored: a mistyped option in a procedure meant
      * for auditing would otherwise silently widen the window being audited, or quietly drop a
      * filter the caller believed was applied.
      *
      * Takes a map because [[extractOptions]] has already handled the null that means "every default".
      */
    def parse(options: Expr.Map, procedureName: String, recognisedKeys: Set[String]): HistoricalOptions = {
      val entries = options.map

      val unknown = entries.keySet -- recognisedKeys
      if (unknown.nonEmpty)
        throw CypherException.Runtime(
          s"`$procedureName` does not recognise the option(s) ${unknown.toVector.sorted.mkString("`", "`, `", "`")}; " +
          s"it accepts ${recognisedKeys.toVector.sorted.mkString("`", "`, `", "`")}",
        )

      def millis(key: String): Option[Milliseconds] = entries.get(key).flatMap {
        case Expr.Null => None
        case Expr.Integer(t) => Some(Milliseconds(t))
        case other =>
          throw CypherException.Runtime(
            s"`$procedureName` expects an integer millisecond timestamp for `$key`, but got: $other",
          )
      }

      def flag(key: String, whenAbsent: Boolean): Boolean = entries.get(key) match {
        case None | Some(Expr.Null) => whenAbsent
        case Some(Expr.True) => true
        case Some(Expr.False) => false
        case Some(other) =>
          throw CypherException.Runtime(
            s"`$procedureName` expects true or false for `$key`, but got: $other",
          )
      }

      val limit = entries.get(Limit).flatMap {
        case Expr.Null => None
        case Expr.Integer(n) if n > 0L && n <= Int.MaxValue.toLong => Some(n.toInt)
        case Expr.Integer(n) =>
          throw CypherException.Runtime(
            s"`$procedureName` expects a positive `$Limit` no greater than ${Int.MaxValue}, but got: $n",
          )
        case other =>
          throw CypherException.Runtime(
            s"`$procedureName` expects an integer for `$Limit`, but got: $other",
          )
      }

      val parsed = HistoricalOptions(
        since = millis(Since),
        through = millis(Through),
        limit = limit,
        unreadableAsNull = flag(UnreadableAsNull, whenAbsent = false),
      )
      for {
        since <- parsed.since
        through <- parsed.through
        if since.millis > through.millis
      } throw CypherException.Runtime(
        s"`$procedureName` was given a `$Since` of ${since.millis} after its `$Through` of ${through.millis}",
      )
      parsed
    }

  }

  /** The moment a procedure should actually read, given the one it was asked for.
    *
    * A procedure cannot report further ahead than the query containing it. A query pinned to a past
    * moment by the `at-time` parameter is a view of the graph as it stood then, so a procedure
    * inside it that read further ahead would put state from after that moment into that view. The
    * two bounds are combined by taking the earlier, which is what [[LiteralOpsGraph]]'s `getJournal`
    * already does for the procedures that read the journal directly.
    */
  def earlierOf(asked: Milliseconds, queryMoment: Option[Milliseconds]): Milliseconds =
    queryMoment.fold(asked)(pinned => if (pinned.millis < asked.millis) pinned else asked)

  /** Read a node argument, given as a node, a QuineId, a pretty-string ID, or raw bytes. */
  def extractNode(value: Value, procedureName: String, position: String)(implicit
    idProvider: QuineIdProvider,
  ): QuineId =
    UserDefinedProcedure
      .extractQuineId(value)
      .getOrElse(
        throw CypherException.Runtime(
          s"`$procedureName` expects a node or node ID as the $position argument, but got: $value",
        ),
      )

  /** Read the edge type argument of an edge procedure, which selects which edges to report.
    *
    * A single string names one edge type. A list names several, and an edge matching any one of them
    * is reported — the value-level counterpart of `MATCH (a)-[:USES|OWNS]->(b)`, which cannot be
    * written here because a procedure argument is a value rather than syntax. `null`, or the
    * argument omitted, means every type.
    *
    * An empty list matches no edge type, which is what "any of nothing" means and what Cypher's own
    * `x IN []` already evaluates to. It is deliberately not an error and deliberately not read as
    * "every type": the list is usually computed rather than written out, so a standing query whose
    * expression happened to yield nothing would otherwise either fail at runtime or silently widen
    * to report every edge in the graph. Reporting nothing is the answer that stays correct.
    *
    * @return the types to keep, or `None` to keep every type
    */
  def extractEdgeTypes(value: Value, procedureName: String, position: String): Option[Set[Symbol]] = {
    def expected(what: String, got: Value): Nothing =
      throw CypherException.Runtime(
        s"`$procedureName` expects $what as the $position argument, but got: $got",
      )

    value match {
      case Expr.Null => None
      case Expr.Str(edgeType) => Some(Set(Symbol(edgeType)))
      case Expr.List(elements) =>
        Some(elements.map {
          case Expr.Str(edgeType) => Symbol(edgeType)
          case other => expected("a list of edge type strings", other)
        }.toSet)
      case other => expected("an edge type string, a list of them, or null", other)
    }
  }

  /** True when an edge type filter can never match, so there is no journal worth reading.
    *
    * Only an empty set of types answers this, and it does so on the arguments alone — no edge of any
    * type can match "any of nothing", whatever the node's history holds. Letting the caller stop
    * here turns a guaranteed-empty answer into no reads at all rather than a full walk that discards
    * everything, which matters most where two journals would otherwise be opened.
    */
  def matchesNoEdgeType(edgeTypeFilter: Option[Set[Symbol]]): Boolean = edgeTypeFilter.exists(_.isEmpty)

  /** Every spelling of an edge direction the REST API accepts, so a direction reads and writes the
    * same way whichever surface it came through (see `DebugOpsRoutes`' edge direction parameter).
    */
  val directionSpellings: Map[String, EdgeDirection] = Map(
    "Outgoing" -> EdgeDirection.Outgoing,
    "outgoing" -> EdgeDirection.Outgoing,
    "out" -> EdgeDirection.Outgoing,
    "Incoming" -> EdgeDirection.Incoming,
    "incoming" -> EdgeDirection.Incoming,
    "in" -> EdgeDirection.Incoming,
    "Undirected" -> EdgeDirection.Undirected,
    "undirected" -> EdgeDirection.Undirected,
    "un" -> EdgeDirection.Undirected,
  )

  /** The canonical spelling reported back, matching what the REST API prints. */
  def directionName(direction: EdgeDirection): String = direction match {
    case EdgeDirection.Outgoing => "Outgoing"
    case EdgeDirection.Incoming => "Incoming"
    case EdgeDirection.Undirected => "Undirected"
  }

  /** Count the journal events a call walks.
    *
    * Placed on the journal stream itself rather than on what the procedure reports, so it measures
    * the work done rather than the answer produced. Comparing the two is what shows how selective a
    * call's filters were.
    */
  def countJournalEvents[A](metrics: JournalWalkMetrics): Flow[A, A, NotUsed] =
    Flow[A].map { event =>
      metrics.journalEventsRead.inc()
      event
    }

  /** Time a call and count what it reported.
    *
    * The timer runs until the stream finishes rather than until it starts, because these procedures
    * return as soon as the first row is ready and keep reading behind it; stopping at the first row
    * would time the wrong thing.
    */
  def measureRows[A](metrics: HistoricalProcedureMetrics): Flow[A, A, NotUsed] =
    Flow[A]
      .map { row =>
        metrics.rowsReported.inc()
        row
      }
      .watchTermination() { (_, done) =>
        // The callback runs when the stream is materialized, so this times the whole call: these
        // procedures return as soon as the first row is ready and keep reading behind it.
        val timing = metrics.timer.time()
        done.onComplete(_ => timing.stop())(ExecutionContext.parasitic)
        NotUsed
      }

  /** Split the options map off a procedure's arguments by position.
    *
    * Options occupy the procedure's last argument position, so a call carries them only when it
    * supplies the full arity. Arguments are positional, as they are everywhere else in Cypher, so a
    * caller who wants options while skipping an earlier argument passes null in its place rather
    * than relying on the map being recognised by its type.
    *
    * @param fullArity how many arguments the procedure takes with options supplied
    */
  def splitPositionalOptions(
    arguments: Seq[Value],
    fullArity: Int,
    procedureName: String,
    recognisedKeys: Set[String],
  ): (Seq[Value], HistoricalOptions) =
    if (arguments.length == fullArity)
      (arguments.init, extractOptions(arguments.last, procedureName, recognisedKeys))
    else (arguments, HistoricalOptions.empty)

  /** Read the options argument, which may be a map or null to accept every default. */
  def extractOptions(value: Value, procedureName: String, recognisedKeys: Set[String]): HistoricalOptions =
    value match {
      case Expr.Null => HistoricalOptions.empty
      case map: Expr.Map => HistoricalOptions.parse(map, procedureName, recognisedKeys)
      case other =>
        throw CypherException.Runtime(
          s"`$procedureName` expects a map of options or null as its last argument, but got: $other",
        )
    }

  /** Cap the rows a procedure returns, when a `limit` was given.
    *
    * Applied to the rows the procedure reports rather than to the journal events behind them, so a
    * limit never truncates the reconstruction of a row that is still being assembled. Because every
    * stage upstream is a stream, taking the cap also stops the work behind it: the cancellation
    * propagates back through the journal reads, so a small limit does a small amount of reading
    * rather than reading everything and discarding the surplus.
    */
  def limitRows[A](options: HistoricalOptions): Flow[A, A, NotUsed] =
    options.limit.fold(Flow[A])(n => Flow[A].take(n.toLong))

  /** Fail unless journal persistence is switched on.
    *
    * Every `history.*` procedure reads its answer out of the journal. With `journalEnabled` off
    * nothing is ever written there, so these procedures would return no rows at all — a result
    * indistinguishable from a node that genuinely never changed. Failing makes the difference
    * between "no history" and "history is not being recorded" visible to the caller.
    */
  def requireJournalEnabled(graph: LiteralOpsGraph, procedureName: String): Unit =
    if (!graph.namespacePersistor.persistenceConfig.journalEnabled)
      throw CypherException.Runtime(
        s"`$procedureName` requires the journal to be enabled, but `quine.persistence.journal-enabled` is false, " +
        "so no history is being recorded",
      )
}
