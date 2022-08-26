package com.thatdot.quine.app.routes

import java.time.Instant

import scala.compat.ExecutionContexts
import scala.concurrent.Future

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.{ByteString, Timeout}

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.graph.{BaseGraph, InMemoryNodeLimit}
import com.thatdot.quine.model.{Milliseconds, QuineId}
import com.thatdot.quine.persistor.PersistenceAgent
import com.thatdot.quine.routes._
import com.thatdot.quine.{BuildInfo => QuineBuildInfo}

trait AdministrationRoutesState {
  def shutdown(): Future[Unit]
}

/** The Akka HTTP implementation of [[AdministrationRoutes]] */
trait AdministrationRoutesImpl
    extends AdministrationRoutes
    with endpoints4s.akkahttp.server.Endpoints
    with endpoints4s.akkahttp.server.JsonEntitiesFromSchemas
    with exts.ServerQuineEndpoints { self: LazyLogging =>

  def graph: BaseGraph
  implicit def timeout: Timeout

  /** Current product version */
  val version: String

  /** Current config */
  def currentConfig: ujson.Value

  /** State in the application */
  val serviceState: AdministrationRoutesState

  /** Should the liveness probe report live? */
  def isLive: Boolean

  /** Should the readiness probe report ready? */
  def isReady: Boolean

  private val buildInfoRoute = buildInfo.implementedBy { _ =>
    val gitCommit: Option[String] = QuineBuildInfo.gitHeadCommit
      .map(_ + (if (QuineBuildInfo.gitUncommittedChanges) "-DIRTY" else ""))
    QuineInfo(
      version,
      gitCommit,
      QuineBuildInfo.gitHeadCommitDate,
      QuineBuildInfo.javaVmName + " " + QuineBuildInfo.javaVersion + " (" + QuineBuildInfo.javaVendor + ")",
      PersistenceAgent.CurrentVersion.shortString
    )
  }

  private val configRoute = config.implementedBy(_ => currentConfig)

  private val livenessProbeRoute = livenessProbe.implementedBy(_ => isLive)

  private val readinessProbeRoute = readinessProbe.implementedBy(_ => isReady)

  private val metricsRoute = metrics.implementedBy { _ =>
    // TODO consider explicitly including relevant counters, timers, and gauges
    // (eg from Metrics object or GlobalQuineMetrics) instead of sorting through
    // on the front-end. Current (kitchen sink backend) implementation is more leaky
    // and bandwidth heavy but allows frontend to pick and choose which metrics it
    // cares about.

    import scala.jdk.CollectionConverters._

    val counters = graph.metrics.metricRegistry.getCounters.asScala.map { case (name, counter) =>
      Counter(name, counter.getCount)
    }
    val timers = graph.metrics.metricRegistry.getTimers.asScala.map { case (name, timer) =>
      val NANOS_IN_MILLI = 1e6
      val snap = timer.getSnapshot
      TimerSummary(
        name,
        min = snap.getMin.toDouble / NANOS_IN_MILLI,
        max = snap.getMax.toDouble / NANOS_IN_MILLI,
        median = snap.getMedian / NANOS_IN_MILLI,
        mean = snap.getMean / NANOS_IN_MILLI,
        q1 = snap.getValue(0.25) / NANOS_IN_MILLI,
        q3 = snap.getValue(0.75) / NANOS_IN_MILLI,
        oneMinuteRate = timer.getOneMinuteRate,
        `90` = snap.getValue(0.90) / NANOS_IN_MILLI,
        `99` = snap.get99thPercentile() / NANOS_IN_MILLI,
        `80` = snap.getValue(0.80) / NANOS_IN_MILLI,
        `20` = snap.getValue(0.20) / NANOS_IN_MILLI,
        `10` = snap.getValue(0.10) / NANOS_IN_MILLI
      )
    }

    val gauges: Seq[NumericGauge] = {
      def coerceDouble[T](value: T): Option[Double] = value match {
        case x: Double => Some(x)
        case x: Float => Some(x.toDouble)
        case x: Long => Some(x.toDouble)
        case x: Int => Some(x.toDouble)
        case x: java.lang.Number => Some(x.doubleValue)
        case _ =>
//            logger.warn("uh oh",
//              new ClassCastException(
//                s"Unable to coerce gauged value $value of type ${value.getClass.getSimpleName} to a numeric type"
//              )
//            )
          None
      }

      (for {
        (name, g) <- graph.metrics.metricRegistry.getGauges.asScala
        v <- coerceDouble(g.getValue)
      } yield NumericGauge(name, v)).toSeq
    }

    MetricsReport(
      Instant.now(),
      counters.toSeq,
      timers.toSeq,
      gauges
    )
  }

  // Deliberately not using `implementedByAsync`. The API will confirm receipt of the request, but not wait for completion.
  private val shutdownRoute = shutdown.implementedBy { _ =>
    graph.shardDispatcherEC.execute(() =>
      Runtime.getRuntime().exit(0)
    ) // `ec.execute` ensures the shutdown request is answered
  }

  private val metaDataRoute = metaData.implementedByAsync { _ =>
    graph.persistor
      .getAllMetaData()
      .map(_.view.map { case (k, v) => k -> ByteString(v) }.toMap)(graph.shardDispatcherEC)
  }

  private val shardSizesRoute = shardSizes.implementedByAsync { resizes =>
    graph
      .shardInMemoryLimits(resizes.mapValues(l => InMemoryNodeLimit(l.softLimit, l.hardLimit)))
      .map(_.collect { case (shardIdx, Some(InMemoryNodeLimit(soft, hard))) =>
        shardIdx -> ShardInMemoryLimit(soft, hard)
      })(ExecutionContexts.parasitic)
  }

  private val requestSleepNodeRoute = requestNodeSleep.implementedByAsync { (quineId: QuineId) =>
    graph.requestNodeSleep(quineId)
  }

  private val graphHashCodeRoute = graphHashCode.implementedByAsync { atTime: Option[Milliseconds] =>
    val at = atTime.getOrElse(Milliseconds.currentTime())
    val ec = ExecutionContexts.parasitic
    graph.getGraphHashCode(Some(at)).map(GraphHashCode(_, at.millis))(ec)
  }

  final val administrationRoutes: Route =
    buildInfoRoute ~
    configRoute ~
    readinessProbeRoute ~
    livenessProbeRoute ~
    metricsRoute ~
    shutdownRoute ~
    metaDataRoute ~
    shardSizesRoute ~
    requestSleepNodeRoute ~
    graphHashCodeRoute
}
