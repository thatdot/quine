package com.thatdot.quine.app.routes

import java.time.Instant

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.util.{ByteString, Timeout}

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json

import com.thatdot.quine.app.config.{BaseConfig, QuineConfig}
import com.thatdot.quine.graph.{BaseGraph, InMemoryNodeLimit}
import com.thatdot.quine.model.Milliseconds
import com.thatdot.quine.persistor.PersistenceAgent
import com.thatdot.quine.routes._
import com.thatdot.quine.{BuildInfo => QuineBuildInfo}

trait AdministrationRoutesState {
  def shutdown()(implicit ec: ExecutionContext): Future[Unit]
}

/** The Pekko HTTP implementation of [[AdministrationRoutes]] */
trait AdministrationRoutesImpl
    extends AdministrationRoutes
    with endpoints4s.pekkohttp.server.Endpoints
    with com.thatdot.quine.app.routes.exts.circe.JsonEntitiesFromSchemas
    with com.thatdot.quine.app.routes.exts.ServerQuineEndpoints { self: LazyLogging =>

  def graph: BaseGraph
  implicit def timeout: Timeout

  /** Current product version */
  val version: String

  /** Current config */
  def currentConfig: Json

  /** State in the application */
  val quineApp: AdministrationRoutesState

  /** A sample configuration that will be used for documenting the admin/config route. */
  def sampleConfig: BaseConfig = QuineConfig()

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

  private val configRoute = config(sampleConfig.loadedConfigJson).implementedBy(_ => currentConfig)

  private val livenessProbeRoute = livenessProbe.implementedBy(_ => ())

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
    graph.shutdown()
    ()
  }

  private val metaDataRoute = metaData.implementedByAsync { _ =>
    graph.namespacePersistor
      .getAllMetaData()
      .map(_.fmap(ByteString(_)))(graph.shardDispatcherEC)
  }

  private val shardSizesRoute = shardSizes.implementedByAsync { resizes =>
    graph
      .shardInMemoryLimits(resizes.fmap(l => InMemoryNodeLimit(l.softLimit, l.hardLimit)))
      .map(_.collect { case (shardIdx, Some(InMemoryNodeLimit(soft, hard))) =>
        shardIdx -> ShardInMemoryLimit(soft, hard)
      })(ExecutionContext.parasitic)
  }

  private val requestSleepNodeRoute = requestNodeSleep.implementedByAsync { case (quineId, namespaceParam) =>
    graph.requestNodeSleep(namespaceFromParam(namespaceParam), quineId)
  }

  private val graphHashCodeRoute = graphHashCode.implementedByAsync { case (atTime, namespaceParam) =>
    val at = atTime.getOrElse(Milliseconds.currentTime())
    val ec = ExecutionContext.parasitic
    graph
      .getGraphHashCode(namespaceFromParam(namespaceParam), Some(at))
      .map(GraphHashCode(_, at.millis))(ec)
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
