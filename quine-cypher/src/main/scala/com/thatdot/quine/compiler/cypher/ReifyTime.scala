package com.thatdot.quine.compiler.cypher

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import scala.concurrent.{ExecutionContext, Future}

import akka.stream.scaladsl.Source
import akka.util.Timeout

import com.thatdot.quine.graph.cypher.{Expr, _}
import com.thatdot.quine.graph.{LiteralOpsGraph, idFrom}
import com.thatdot.quine.model.{QuineId, QuineIdProvider, QuineValue}

object ReifyTime extends UserDefinedProcedure {
  val name = "reify.time"
  val canContainUpdates = true
  val isIdempotent = true
  val canContainAllNodeScan = false

  val signature: UserDefinedProcedureSignature =
    UserDefinedProcedureSignature(
      arguments = Seq(
        "timestamp" -> Type.DateTime,
        "periods" -> Type.List(Type.Str)
      ),
      outputs = Vector("node" -> Type.Node),
      description = "Returns the nodes that represent the passed-in time."
    )

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation
  )(implicit
    ec: ExecutionContext,
    parameters: Parameters,
    timeout: Timeout
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
    val periodsFiltered = (periodKeySet.map(_.map(_.toLowerCase)) match {
      case Some(pks) =>
        // Validate every period is defined
        val allPeriodKeys = allPeriods.map(_._1)
        if (!pks.forall(allPeriodKeys.contains))
          throw wrongSignature(arguments)
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
    import location._

    val formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME

    // Generate a QuineId from a values that define a time node key (time and period)
    def timeNodeId(
      sourceTime: ZonedDateTime,
      period: Period
    ): QuineId = {
      val periodTruncatedDate = period.truncate(sourceTime)
      val periodTruncatedDateStr = periodTruncatedDate.format(formatter)
      idFrom(Expr.Str("time-node"), Expr.Str(period.name), Expr.Str(periodTruncatedDateStr))
    }

    // For the provided time and period, generate a time node and return its ID.
    // Additionally, generates graph.literalOps API calls to setup time nodes and relatives (next, and parent).
    // Returns a Future that completes when the graph updates are complete.
    def generateTimeNode(
      sourceTime: ZonedDateTime,
      period: Period,
      parentPeriod: Option[Period]
    ): (QuineId, Future[Unit]) = {
      val periodTruncatedDate = period.truncate(sourceTime)
      val previousPeriodSourceTime = period.previous(sourceTime)
      val nextPeriodSourceTime = period.next(sourceTime)
      val nodeId = timeNodeId(sourceTime, period)
      val previousNodeId = timeNodeId(previousPeriodSourceTime, period)
      val nextNodeId = timeNodeId(nextPeriodSourceTime, period)
      val parentNodeId = parentPeriod.map(timeNodeId(sourceTime, _))
      val effects = Future
        .sequence(
          List(
            graph.literalOps.setLabel(nodeId, period.name + ":" + period.labelFormat.format(sourceTime)),
            graph.literalOps
              .setLabel(previousNodeId, period.name + ":" + period.labelFormat.format(previousPeriodSourceTime)),
            graph.literalOps.setLabel(nextNodeId, period.name + ":" + period.labelFormat.format(nextPeriodSourceTime)),
            graph.literalOps.setProp(nodeId, "period", QuineValue.Str(period.name)),
            graph.literalOps.setProp(nodeId, "start", QuineValue.DateTime(periodTruncatedDate.toInstant)),
            graph.literalOps.addEdge(nodeId, nextNodeId, "next"),
            graph.literalOps.addEdge(previousNodeId, nodeId, "next")
          ) ::: (parentNodeId match {
            case Some(pid) =>
              List(graph.literalOps.addEdge(pid, nodeId, period.name))
            case None => List.empty
          })
        )
        .map(_ => ())
      (nodeId, effects)
    }

    // Generate time node for each of the user's periods
    val generateTimeNodeResult = for {
      (period, parentPeriod) <- periodsWithParents
      (nodeId, effects) = generateTimeNode(dt, period, parentPeriod)
    } yield (nodeId, effects)

    // Source containing the time node generated for each of the user's periods
    // This only includes nodes that follow the parent hierarchy, not nodes connected by next
    val timeNodeSource = Source
      .fromIterator(() => generateTimeNodeResult.map(_._1).iterator)
      .mapAsync(parallelism = 1)(UserDefinedProcedure.getAsCypherNode(_, atTime, graph))
      .map(Vector(_))

    // Return a source that blocks until graph node commands have been responded to
    Source
      .future(Future.sequence(generateTimeNodeResult.map(_._2)))
      .map(_ => Left(()))
      .concat(timeNodeSource.map(Right(_)))
      .dropWhile(_.isLeft)
      .map(_.right.get)
  }

  private type PeriodKey = String

  private trait Period {
    val name: String
    def truncate(z: ZonedDateTime): ZonedDateTime
    def previous(z: ZonedDateTime): ZonedDateTime
    def next(z: ZonedDateTime): ZonedDateTime
    val labelFormat: DateTimeFormatter
  }

  private val allPeriods: Seq[(PeriodKey, Period)] = Seq(
    "year" -> new Period {
      val name = "year"
      def truncate(z: ZonedDateTime): ZonedDateTime =
        z.withNano(0).withSecond(0).withMinute(0).withHour(0).withDayOfMonth(1).withMonth(1)
      def previous(z: ZonedDateTime): ZonedDateTime = z.minusYears(1)
      def next(z: ZonedDateTime): ZonedDateTime = z.plusYears(1)
      val labelFormat = DateTimeFormatter.ofPattern("yyyy")
    },
    "month" -> new Period {
      val name = "month"
      def truncate(z: ZonedDateTime) =
        z.withNano(0).withSecond(0).withMinute(0).withHour(0).withDayOfMonth(1)
      def previous(z: ZonedDateTime) = z.minusMonths(1)
      def next(z: ZonedDateTime) = z.plusMonths(1)
      val labelFormat = DateTimeFormatter.ofPattern("MM")
    },
    "day" -> new Period {
      val name = "day"
      def truncate(z: ZonedDateTime): ZonedDateTime = z.withNano(0).withSecond(0).withMinute(0).withHour(0)
      def previous(z: ZonedDateTime): ZonedDateTime = z.minusDays(1)
      def next(z: ZonedDateTime): ZonedDateTime = z.plusDays(1)
      val labelFormat = DateTimeFormatter.ofPattern("dd")
    },
    "hour" -> new Period {
      val name = "hour"
      def truncate(z: ZonedDateTime) = z.withNano(0).withSecond(0).withMinute(0)
      def previous(z: ZonedDateTime) = z.minusHours(1)
      def next(z: ZonedDateTime) = z.plusHours(1)
      val labelFormat = DateTimeFormatter.ofPattern("HH")
    },
    "minute" -> new Period {
      val name = "minute"
      def truncate(z: ZonedDateTime) = z.withNano(0).withSecond(0)
      def previous(z: ZonedDateTime) = z.minusMinutes(1)
      def next(z: ZonedDateTime) = z.plusMinutes(1)
      val labelFormat = DateTimeFormatter.ofPattern("mm")
    },
    "second" -> new Period {
      val name = "second"
      def truncate(z: ZonedDateTime) = z.withNano(0)
      def previous(z: ZonedDateTime) = z.minusSeconds(1)
      def next(z: ZonedDateTime) = z.plusSeconds(1)
      val labelFormat = DateTimeFormatter.ofPattern("ss")
    }
  )
}
