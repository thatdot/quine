package com.thatdot.quine.webapp.components.landing

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.{MetricsReport, ShardInMemoryLimit, TimerSummary}
import com.thatdot.quine.webapp.components.dashboard.Card
import com.thatdot.quine.webapp.components.landing.V2ApiTypes._
import com.thatdot.quine.webapp.util.Pot

/** Full-width card displaying the animated system overview flow diagram.
  *
  * Reactively re-renders the D3 diagram when ingest, standing query, config, or
  * metrics data changes. Shows a loading placeholder until the required data
  * sources arrive. Metrics are optional — when unavailable (no permission or
  * before first fetch), the persistor renders with zero rate/latency.
  */
object SystemOverviewCard {

  type MetricsData = (MetricsReport, Map[Int, ShardInMemoryLimit])

  val requiredPermissions: Set[String] = Set("IngestRead", "StandingQueryRead", "ApplicationStateRead")

  def apply(
    ingestsSignal: Signal[Pot[Seq[V2IngestInfo]]],
    standingQueriesSignal: Signal[Pot[Seq[V2StandingQueryInfo]]],
    configSignal: Signal[Pot[V2QuineConfig]],
    metricsSignal: Signal[Pot[MetricsData]],
    clusterStatusSignal: Option[Signal[Pot[V2ServiceStatus]]] = None,
  ): HtmlElement = {
    // `None` when the caller didn't wire a cluster-status signal (OSS). We fall back
    // to a ready signal of `Pot.Empty` so `combine` still produces updates on the
    // other inputs.
    val clusterSig: Signal[Pot[V2ServiceStatus]] =
      clusterStatusSignal.getOrElse(Signal.fromValue(Pot.Empty: Pot[V2ServiceStatus]))

    val diagramSignal: Signal[HtmlElement] =
      Signal
        .combine(ingestsSignal, standingQueriesSignal, configSignal, metricsSignal, clusterSig)
        .map { case (ingestsPot, sqPot, configPot, metricsPot, clusterPot) =>
          (ingestsPot.toOption, sqPot.toOption, configPot.toOption) match {
            case (Some(apiIngests), Some(apiQueries), Some(config)) =>
              OverviewDiagram(
                ingests = LandingPageData.fromV2Ingests(apiIngests),
                queries = LandingPageData.fromV2StandingQueries(apiQueries),
                persistor = persistorFromConfig(config, metricsPot.toOption.map(_._1)),
                clusterFullyUp = clusterPot.toOption.map(_.fullyUp),
              )
            case _ =>
              loadingPlaceholder
          }
        }

    Card(
      title = "System Overview",
      body = div(child <-- diagramSignal),
    )
  }

  private def persistorFromConfig(config: V2QuineConfig, metrics: Option[MetricsReport]): PersistorInfo = {
    val base = PersistorInfo.empty.copy(
      name = prettyStoreName(config.storeType),
      status = "Healthy",
    )
    metrics.fold(base) { m =>
      val writeT = findTimer(m, "persistor.persist-event")
      val readT = findTimer(m, "persistor.get-journal")
      base.copy(
        writeLatencyMs = writeT.map(_.mean).getOrElse(0.0),
        writeOpsPerSec = writeT.map(_.oneMinuteRate).getOrElse(0.0),
        readLatencyMs = readT.map(_.mean).getOrElse(0.0),
        readOpsPerSec = readT.map(_.oneMinuteRate).getOrElse(0.0),
      )
    }
  }

  /** Timer names in `MetricsReport` are fully-qualified (e.g. "shared.persistor.persist-event").
    * Match on suffix so we pick up whichever namespace the host reports.
    */
  private def findTimer(metrics: MetricsReport, suffix: String): Option[TimerSummary] =
    metrics.timers.find(t => t.name == suffix || t.name.endsWith("." + suffix))

  private def prettyStoreName(slug: String): String = slug.toLowerCase match {
    case "cassandra" => "Cassandra"
    case "rocksdb" => "RocksDB"
    case "mapdb" => "MapDB"
    case "keyspaces" => "Keyspaces"
    case "clickhouse" => "ClickHouse"
    case "berkeleydb" | "berkeley-db" => "BerkeleyDB"
    case "empty" => "Empty"
    case "in-memory" | "inmemory" => "In-Memory"
    case other => other.capitalize
  }

  private val loadingPlaceholder: HtmlElement =
    div(
      cls := "d-flex align-items-center justify-content-center text-muted",
      styleAttr := "min-height: 300px;",
      span(cls := "spinner-border spinner-border-sm me-2"),
      span("Loading system overview..."),
    )
}
