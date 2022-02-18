package com.thatdot.quine.app.config

import java.io.File

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.{CsvReporter, MetricRegistry, Reporter, ScheduledReporter, Slf4jReporter}
import metrics_influxdb.api.measurements.MetricMeasurementTransformer
import metrics_influxdb.{HttpInfluxdbProtocol, InfluxdbReporter}
import org.slf4j.LoggerFactory
import pureconfig.generic.FieldCoproductHint

abstract class ReporterWrapper(reporter: Reporter) {
  def start(): Unit
  def stop(): Unit = reporter.close()
}
class ScheduledReporterWrapper(period: FiniteDuration, reporter: ScheduledReporter) extends ReporterWrapper(reporter) {
  def start(): Unit = reporter.start(period.length, period.unit)
}
class JmxReporterWrapper(reporter: JmxReporter) extends ReporterWrapper(reporter) {
  def start(): Unit = reporter.start()
}

/** Class to represent config values corresponding to Dropwizard Metrics implementations.
  */
sealed abstract class MetricsReporter {

  /** Register the reporter for a given MetricRegistry
    *
    * @param registry  registry of metrics on which reporter should report
    * @param namespace namespace under which to report metrics
    * @return          wrapper for the reporter with start() and stop() methods.
    */
  def register(registry: MetricRegistry, namespace: String): ReporterWrapper
}
object MetricsReporter {
  // This is so 'Slf4j' doesn't get turned into 'slf-4j' by the default impl
  implicit val metricsReporterNameHint: FieldCoproductHint[MetricsReporter] =
    new FieldCoproductHint[MetricsReporter]("type") {
      override def fieldValue(name: String): String = name.toLowerCase
    }

  case object Jmx extends MetricsReporter {
    def register(registry: MetricRegistry, namespace: String): ReporterWrapper =
      new JmxReporterWrapper(JmxReporter.forRegistry(registry).build())

  }
  sealed abstract class PeriodicReporter extends MetricsReporter {
    def period: FiniteDuration
    protected def wrapReporter(reporter: ScheduledReporter): ReporterWrapper =
      new ScheduledReporterWrapper(period, reporter)
  }
  final case class Csv(period: FiniteDuration, logDirectory: File) extends PeriodicReporter {
    def register(registry: MetricRegistry, namespace: String): ReporterWrapper = {
      logDirectory.mkdir()
      wrapReporter(CsvReporter.forRegistry(registry).build(logDirectory))
    }
  }
  final case class Slf4j(period: FiniteDuration, loggerName: String = "metrics") extends PeriodicReporter {
    def register(registry: MetricRegistry, namespace: String): ReporterWrapper = wrapReporter(
      Slf4jReporter.forRegistry(registry).outputTo(LoggerFactory.getLogger(loggerName)).build()
    )
  }
  final case class Influxdb(
    period: FiniteDuration,
    database: String = "metrics",
    scheme: String = "http",
    host: String = "localhost",
    port: Int = 8086,
    user: Option[String] = None,
    password: Option[String] = None
  ) extends PeriodicReporter {
    def register(registry: MetricRegistry, namespace: String): ReporterWrapper = wrapReporter(
      InfluxdbReporter
        .forRegistry(registry)
        .protocol(
          new HttpInfluxdbProtocol(scheme, host, port, user.orNull, password.orNull, database)
        )
        .withAutoCreateDB(true)
        .transformer(new TagInfluxMetrics(Map("member_id" -> namespace)))
        .build()
    )
  }
}

class TagInfluxMetrics(tags: Map[String, String]) extends MetricMeasurementTransformer {
  override def tags(metricName: String): java.util.Map[String, String] = tags.asJava

  override def measurementName(metricName: String): String = metricName
}
