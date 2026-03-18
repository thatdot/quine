package com.thatdot.quine.webapp.components.dashboard

import java.time.ZoneId
import java.time.format.DateTimeFormatter

import scala.collection.SortedSet
import scala.math.BigDecimal.RoundingMode

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.{Counter, MetricsReport, ShardInMemoryLimit}

/** Shared rendering logic for the metrics dashboard. Each product provides its
  * own polling logic and feeds a `MetricsResult` stream into `renderDashboard`.
  */
object MetricsDashboardRenderer {

  /** The result of a metrics poll. Left is an error message, Right is the metrics data. */
  type MetricsResult = Either[String, (MetricsReport, Map[Int, ShardInMemoryLimit])]

  def extractShardIds(counters: Seq[Counter]): SortedSet[Int] = SortedSet(counters.collect {
    case Counter(ShardInfoCard.ShardCounterName(shardId, _, _), _) => shardId.toInt
  }: _*)

  private def extractHistogramCard(
    histogramName: String,
    counters: Seq[Counter],
  ): Option[(String, HtmlElement)] = {
    val countersForName = counters.filter { ctr =>
      ctr.name match {
        case CounterSummaryCard.BucketLabel(_, candidateName, _, _) =>
          histogramName == candidateName
        case _ => false
      }
    }

    countersForName.collectFirst { case Counter(CounterSummaryCard.BucketLabel(fullBucketName, _, _, _), _) =>
      fullBucketName -> CounterSummaryCard(fullBucketName, countersForName)
    }
  }

  private def extractMemoryCards(metrics: MetricsReport): Seq[(String, HtmlElement)] = {
    val totalGauges = ("Total Memory Usage", "memory.total.used", "memory.total.max")
    val heapGauges = ("Heap Usage", "memory.heap.used", "memory.heap.max")
    Vector(totalGauges, heapGauges).flatMap { case (title, currGaugeName, maxGaugeName) =>
      def normalizeMb(bytes: Double): Double = {
        val MB_IN_B = 1024 * 1024
        (BigDecimal(bytes) / MB_IN_B).setScale(3, RoundingMode.HALF_UP).toDouble
      }
      for {
        maxGauge <- metrics.gauges.find(_.name == maxGaugeName)
        if maxGauge.value > 0
        maxGaugeVal = normalizeMb(maxGauge.value)
        currGauge <- metrics.gauges.find(_.name == currGaugeName)
        currGaugeVal = normalizeMb(currGauge.value)
      } yield title -> Card(
        title = title,
        body = ProgressBarMeter(
          name = "MB",
          value = currGaugeVal,
          softMax = maxGaugeVal,
          hardMax = maxGaugeVal,
        ),
      )
    }
  }

  /** Render a complete metrics dashboard from a stream of polling results.
    *
    * @param metricsStream stream of Either[errorMessage, (metrics, shardSizes)]
    */
  def renderDashboard(metricsStream: EventStream[MetricsResult]): HtmlElement = {
    val metricsVar: Var[MetricsReport] = Var(MetricsReport.empty)
    val shardSizesVar: Var[Map[Int, ShardInMemoryLimit]] = Var(Map.empty)
    val errorMessageVar: Var[Option[String]] = Var(None)
    val advancedVar: Var[Boolean] = Var(false)

    div(
      padding := "1em",
      metricsStream --> {
        case Left(errorMsg) =>
          errorMessageVar.set(Some(errorMsg))
        case Right((newMetrics, newShardSizes)) =>
          errorMessageVar.set(None)
          metricsVar.set(newMetrics)
          shardSizesVar.set(newShardSizes)
      },
      h2(cls := "px-3 h2", "System Dashboard"),
      div(
        cls := "dashboard grid px-3",
        // Header row
        div(
          cls := "row",
          div(
            cls := "col-12 mt-3",
            child.text <-- metricsVar.signal.map { metrics =>
              s"Data accurate as of ${metrics.atTime.atZone(ZoneId.of("GMT")).format(DateTimeFormatter.RFC_1123_DATE_TIME)}"
            },
            div(
              cls := "float-end",
              label(
                "Advanced debugging: ",
                input(
                  typ := "checkbox",
                  controlled(
                    checked <-- advancedVar.signal,
                    onClick.mapToChecked --> advancedVar,
                  ),
                ),
              ),
            ),
          ),
        ),
        // Error row
        child <-- errorMessageVar.signal.map {
          case Some(msg) =>
            div(cls := "row", div(cls := "col-12 mt-3", cls := "text-danger", msg))
          case None =>
            div(cls := "row")
        },
        // Memory info row
        div(
          cls := "row",
          children <-- metricsVar.signal.map { metrics =>
            extractMemoryCards(metrics).map { case (_, memoryCard) =>
              div(cls := "col-12 col-md-6 col-xl-3 mt-3", memoryCard)
            }
          },
        ),
        // Shard info row
        div(
          cls := "row",
          children <-- metricsVar.signal.combineWith(shardSizesVar.signal, advancedVar.signal).map {
            case (metrics, shardSizes, advanced) =>
              extractShardIds(metrics.counters).toSeq.flatMap { shardId =>
                ShardInfoCard.ShardInfo.forShard(shardId, metrics.counters, shardSizes.get(shardId)).map { info =>
                  div(cls := "col-12 col-md-6 col-xl-3 mt-3", ShardInfoCard(info, advanced))
                }
              }
          },
        ),
        // Binary histograms row
        div(
          cls := "row",
          children <-- metricsVar.signal.map { metrics =>
            Vector("property-counts", "edge-counts")
              .flatMap(extractHistogramCard(_, metrics.counters))
              .map { case (_, histogramCard) =>
                div(cls := "col-12 col-md-6 mt-3", histogramCard)
              }
          },
        ),
        // Timers row
        div(
          cls := "row",
          children <-- metricsVar.signal.map { metrics =>
            metrics.timers.map { timer =>
              div(cls := "col-12 col-md-6 col-xl-3 mt-3", TimerSummaryCard(timer))
            }
          },
        ),
      ),
    )
  }
}
