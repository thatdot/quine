package com.thatdot.quine.compiler.cypher

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME

import scala.collection.Set
import scala.concurrent.Future

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import com.thatdot.quine.graph.cypher.CypherException.ConstraintViolation
import com.thatdot.quine.graph.cypher._
import com.thatdot.quine.graph.{LiteralOpsGraph, idFrom}
import com.thatdot.quine.model.{QuineId, QuineIdProvider, QuineValue}
import com.thatdot.quine.util.Log._

object ReifyTime extends UserDefinedProcedure {
  val name = "reify.time"
  val canContainUpdates = true
  val isIdempotent = true
  val canContainAllNodeScan = false

  val signature: UserDefinedProcedureSignature =
    UserDefinedProcedureSignature(
      arguments = Seq(
        "timestamp" -> Type.DateTime,
        "periods" -> Type.List(Type.Str),
      ),
      outputs = Vector("node" -> Type.Node),
      description = """Reifies the timestamp into a [sub]graph of time nodes, where each node represents one
                      |period (at the granularity of the period specifiers provided). Yields the reified nodes
                      |with the finest granularity.""".stripMargin.replace('\n', ' '),
    )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation,
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig,
  ): Source[Vector[Value], _] = {

    // Read call arguments and default values
    val (dt: ZonedDateTime, periodKeySet: Option[Set[PeriodKey]]) = arguments match {
      case Vector() => (ZonedDateTime.now, None)
      case Vector(Expr.DateTime(a)) => (a, None)
      case Vector(Expr.DateTime(a), Expr.List(b)) =>
        if (b.exists(_.typ != Type.Str)) throw wrongSignature(arguments)
        (a, Some(b collect { case Expr.Str(c) => c } toSet))
      case Vector(Expr.List(b)) =>
        if (b.exists(_.typ != Type.Str)) throw wrongSignature(arguments)
        (ZonedDateTime.now, Some(b collect { case Expr.Str(c) => c } toSet))
      case Vector(Expr.Str(a)) =>
        (ZonedDateTime.now, Some(Set(a)))
      case _ => throw wrongSignature(arguments)
    }

    // Determine periods to use. Normalize user input to lowercase for ease of use.
    val periodsFiltered: Seq[Period] = (periodKeySet.map(_.map(_.toLowerCase)) match {
      case Some(pks) =>
        if (pks.isEmpty)
          throw ConstraintViolation("Argument 'periods' must not be empty")
        // Validate every period is defined
        val allPeriodKeys = allPeriods.map(_._1)
        if (!pks.forall(allPeriodKeys.contains))
          throw ConstraintViolation(
            "Argument 'periods' must contain only valid period specifiers (eg, 'year', 'minute', etc.)",
          )
        // Filter allPeriods in order to preserve order
        allPeriods.filter(p => pks.contains(p._1))
      case None => allPeriods
    }).map(_._2)

    // Associate each period with its parent period within the user-configured periods
    val periodsWithParents = {
      var last: Option[Period] = None
      periodsFiltered.map { p =>
        val result = (p, last)
        last = Some(p)
        result
      }
    }

    implicit val graph: LiteralOpsGraph = LiteralOpsGraph.getOrThrow(s"`$name` procedure", location.graph)
    implicit val idProvider: QuineIdProvider = location.idProvider

    // Generate a QuineId from a values that define a time node key (time and period)
    def timeNodeId(
      sourceTime: ZonedDateTime,
      period: Period,
    ): QuineId = {
      val periodTruncatedDate = period.truncate(sourceTime)
      val periodTruncatedDateStr = periodTruncatedDate.format(ISO_OFFSET_DATE_TIME)
      idFrom(Expr.Str("time-node"), Expr.Str(period.name), Expr.Str(periodTruncatedDateStr))
    }

    // For the provided time and period, generate a time node and return its ID.
    // Additionally, generates graph.literalOps API calls to setup time nodes and relatives (next, and parent).
    // Returns a Future that completes when the graph updates are complete.
    def generateTimeNode(
      sourceTime: ZonedDateTime,
      period: Period,
      parentPeriod: Option[Period],
    ): (QuineId, Future[Unit]) = {
      val periodTruncatedDate = period.truncate(sourceTime)
      val previousPeriodSourceTime = period.truncate(period.previous(sourceTime))
      val nextPeriodSourceTime = period.truncate(period.next(sourceTime))
      val nodeId = timeNodeId(sourceTime, period)
      val previousNodeId = timeNodeId(previousPeriodSourceTime, period)
      val nextNodeId = timeNodeId(nextPeriodSourceTime, period)
      val parentNodeId = parentPeriod.map(timeNodeId(sourceTime, _))

      //TODO GROSS!!!!! 🤮
      implicit val localEC = location.graph.nodeDispatcherEC

      val effects = Future
        .sequence(
          List(
            // set a label on each of prev, this, next
            graph.literalOps(location.namespace).setLabel(nodeId, period.name),
            graph
              .literalOps(location.namespace)
              .setLabel(previousNodeId, period.name),
            graph.literalOps(location.namespace).setLabel(nextNodeId, period.name),
            // set each of prev.period, this.period, next.period
            graph.literalOps(location.namespace).setProp(nodeId, "period", QuineValue.Str(period.name)),
            graph.literalOps(location.namespace).setProp(previousNodeId, "period", QuineValue.Str(period.name)),
            graph.literalOps(location.namespace).setProp(nextNodeId, "period", QuineValue.Str(period.name)),
            // set each of prev.start, this.start, next.period
            graph
              .literalOps(location.namespace)
              .setProp(nodeId, "start", QuineValue.DateTime(periodTruncatedDate.toOffsetDateTime)),
            graph
              .literalOps(location.namespace)
              .setProp(previousNodeId, "start", QuineValue.DateTime(previousPeriodSourceTime.toOffsetDateTime)),
            graph
              .literalOps(location.namespace)
              .setProp(nextNodeId, "start", QuineValue.DateTime(nextPeriodSourceTime.toOffsetDateTime)),
            // edges (prev)->(this)->(next)
            graph.literalOps(location.namespace).addEdge(nodeId, nextNodeId, "NEXT"),
            graph.literalOps(location.namespace).addEdge(previousNodeId, nodeId, "NEXT"),
          ) ::: (parentNodeId match {
            case Some(pid) =>
              val periodEdgeName = period.name.toUpperCase
              List(graph.literalOps(location.namespace).addEdge(pid, nodeId, periodEdgeName))
            case None => List.empty
          }),
        ) //(implicitly, location.graph.nodeDispatcherEC)
        .map(_ => ())(location.graph.nodeDispatcherEC)
      (nodeId, effects)
    }

    // Generate time node for each of the user's periods
    val generateTimeNodeResult = for {
      (period, parentPeriod) <- periodsWithParents
      (nodeId, effects) = generateTimeNode(dt, period, parentPeriod)
    } yield (nodeId, effects)

    // Source containing the time node generated at the user's smallest granularity time period
    // This only includes nodes that follow the parent hierarchy, not nodes connected by next
    val timeNodeSource = Source
      .single(generateTimeNodeResult.last._1)
      .mapAsync(parallelism = 1)(UserDefinedProcedure.getAsCypherNode(_, location.namespace, location.atTime, graph))
      .map(Vector(_))

    //TODO GROSS!!!!! 🤮
    implicit val localEC = location.graph.nodeDispatcherEC

    // Return a source that blocks until graph node commands have been responded to,
    // and contains the single smallest period time node
    Source
      .future(Future.sequence(generateTimeNodeResult.map(_._2))) //(implicitly, location.graph.nodeDispatcherEC))
      .map(_ => Left(()))
      .concat(timeNodeSource.map(Right(_)))
      .dropWhile(_.isLeft)
      .map(_.toOption.get)
  }

  private type PeriodKey = String

  private trait Period {
    val name: String
    def truncate(z: ZonedDateTime): ZonedDateTime
    def previous(z: ZonedDateTime): ZonedDateTime
    def next(z: ZonedDateTime): ZonedDateTime
    val labelFormat: DateTimeFormatter
  }

  /** Keys are the values that may be specified as periods (ie, entries in the 'periods" list argument)
    * This sequence must be ordered by increasing granularity, as its order is used to determine which nodes to yield.
    */
  private val allPeriods: Seq[(PeriodKey, Period)] = Seq(
    "year" -> new Period {
      val name = "year"
      def truncate(z: ZonedDateTime): ZonedDateTime =
        z.withNano(0).withSecond(0).withMinute(0).withHour(0).withDayOfMonth(1).withMonth(1)
      def previous(z: ZonedDateTime): ZonedDateTime = z.minusYears(1)
      def next(z: ZonedDateTime): ZonedDateTime = z.plusYears(1)
      val labelFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy")
    },
    "month" -> new Period {
      val name = "month"
      def truncate(z: ZonedDateTime): ZonedDateTime =
        z.withNano(0).withSecond(0).withMinute(0).withHour(0).withDayOfMonth(1)
      def previous(z: ZonedDateTime): ZonedDateTime = z.minusMonths(1)
      def next(z: ZonedDateTime): ZonedDateTime = z.plusMonths(1)
      val labelFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("MM")
    },
    "day" -> new Period {
      val name = "day"
      def truncate(z: ZonedDateTime): ZonedDateTime = z.withNano(0).withSecond(0).withMinute(0).withHour(0)
      def previous(z: ZonedDateTime): ZonedDateTime = z.minusDays(1)
      def next(z: ZonedDateTime): ZonedDateTime = z.plusDays(1)
      val labelFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("dd")
    },
    "hour" -> new Period {
      val name = "hour"

      def truncate(z: ZonedDateTime): ZonedDateTime = z.withNano(0).withSecond(0).withMinute(0)
      def previous(z: ZonedDateTime): ZonedDateTime = z.minusHours(1)
      def next(z: ZonedDateTime): ZonedDateTime = z.plusHours(1)
      val labelFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("HH")
    },
    "minute" -> new Period {
      val name = "minute"
      def truncate(z: ZonedDateTime): ZonedDateTime = z.withNano(0).withSecond(0)
      def previous(z: ZonedDateTime): ZonedDateTime = z.minusMinutes(1)
      def next(z: ZonedDateTime): ZonedDateTime = z.plusMinutes(1)
      val labelFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("mm")
    },
    "second" -> new Period {
      val name = "second"
      def truncate(z: ZonedDateTime): ZonedDateTime = z.withNano(0)
      def previous(z: ZonedDateTime): ZonedDateTime = z.minusSeconds(1)
      def next(z: ZonedDateTime): ZonedDateTime = z.plusSeconds(1)
      val labelFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("ss")
    },
  )
}
