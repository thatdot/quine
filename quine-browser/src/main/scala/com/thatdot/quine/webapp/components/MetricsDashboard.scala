package com.thatdot.quine.webapp.components

import java.time.ZoneId
import java.time.format.DateTimeFormatter

import scala.collection.SortedSet
import scala.concurrent.duration.DurationInt
import scala.math.BigDecimal.RoundingMode
import scala.scalajs.js
import scala.scalajs.js.timers.{SetTimeoutHandle, clearTimeout, setTimeout}
import scala.util.{Failure, Success}

import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._
import slinky.core.Component
import slinky.core.annotations.react
import slinky.core.facade.ReactElement
import slinky.web.html._

import com.thatdot.quine.routes.{ClientRoutes, Counter, MetricsReport, ShardInMemoryLimit}
import com.thatdot.quine.webapp.SlinkyReadWriteInstances._
import com.thatdot.quine.webapp.components.dashboard._
object MetricsDashboard {
  def extractShardIds(counters: Seq[Counter]): SortedSet[Int] = SortedSet(counters.collect {
    case Counter(ShardInfoCard.ShardCounterName(shardId, _, _), _) => shardId.toInt
  }: _*)
}
@react class MetricsDashboard extends Component {

  case class Props(routes: ClientRoutes)
  case class State(
    refresh: Option[SetTimeoutHandle],
    errorMessage: Option[String],
    metrics: MetricsReport,
    shardSizes: Map[Int, ShardInMemoryLimit],
    advanced: Boolean
  )

  override def initialState: State = State(
    refresh = None,
    errorMessage = None,
    metrics = MetricsReport.empty,
    shardSizes = Map.empty,
    advanced = false
  )

  override def componentDidMount(): Unit = {
    setState(_.copy(refresh = Some(pollMetrics()))) // Start polling
    super.componentDidMount()
  }

  override def componentWillUnmount(): Unit =
    state.refresh.foreach(handle => clearTimeout(handle))

  def pollMetrics(): SetTimeoutHandle = setTimeout(2.seconds) {
    val metricsF = props.routes.metrics(()).future
    val shardSizesF = props.routes.shardSizes(Map.empty).future
    metricsF.zip(shardSizesF).onComplete {
      case Failure(exception) =>
        val errorMsg = if (exception.getMessage.isEmpty) {
          "Failed to read metrics from server"
        } else {
          s"Failed to read metrics from server: ${exception.getMessage}"
        }
        setState(
          _.copy(
            errorMessage = Some(errorMsg),
            refresh = Some(pollMetrics())
          )
        )

      case Success((newMetrics, newShardSizes)) =>
        setState(
          _.copy(
            errorMessage = None,
            refresh = Some(pollMetrics()),
            metrics = newMetrics,
            shardSizes = newShardSizes
          )
        )
    }
  }

  // construct a CounterSummaryCard for the provided histogram name, if it exists
  def extractHistogramCard(histogramName: String): Option[(String, ReactElement)] = {
    val countersForName = state.metrics.counters.filter { ctr =>
      ctr.name match {
        case CounterSummaryCard.BucketLabel(_, candidateName, _, _) =>
          histogramName == candidateName
        case _ => false
      }
    }

    countersForName.collectFirst { case Counter(CounterSummaryCard.BucketLabel(fullBucketName, _, _, _), _) =>
      val card = CounterSummaryCard(fullBucketName, countersForName)
      fullBucketName -> card
    }
  }

  // construct bootstrap Cards showing information about memory usage
  def extractMemoryCards(): Seq[(String, ReactElement)] = {
    val totalGauges = ("Total Memory Usage", "memory.total.used", "memory.total.max")
    val heapGauges = ("Heap Usage", "memory.heap.used", "memory.heap.max")
    Vector(totalGauges, heapGauges).flatMap { case (title, currGaugeName, maxGaugeName) =>
      // normalizes a Double representing bytes to a Double representing MB with KB-level precision
      def normalizeMb(bytes: Double): Double = {
        val MB_IN_B = 1024 * 1024
        (BigDecimal(bytes) / MB_IN_B).setScale(3, RoundingMode.HALF_UP).toDouble
      }
      for {
        maxGauge <- state.metrics.gauges.find(_.name == maxGaugeName)
        if maxGauge.value > 0
        maxGaugeVal = normalizeMb(maxGauge.value)
        currGauge <- state.metrics.gauges.find(_.name == currGaugeName)
        currGaugeVal = normalizeMb(currGauge.value)
      } yield {
        val card: ReactElement = Card(
          title = title,
          body = ProgressBarMeter(
            name = "MB",
            value = currGaugeVal,
            softMax = maxGaugeVal,
            hardMax = maxGaugeVal
          )
        )
        title -> card
      }
    }
  }

  // NB classes used expect bootstrap to be present on the rendered page.
  override def render(): ReactElement = {
    val timers: Seq[(String, ReactElement)] =
      state.metrics.timers.map { timer =>
        val card: ReactElement = TimerSummaryCard(timer).withKey(timer.name)
        timer.name -> card
      }

    val histograms: Seq[(String, ReactElement)] =
      Vector("property-counts", "edge-counts")
        .flatMap(extractHistogramCard)

    val shardInfos =
      MetricsDashboard
        .extractShardIds(state.metrics.counters)
        .toSeq
        .flatMap { shardId =>
          ShardInfoCard.ShardInfo.forShard(shardId, state.metrics.counters, state.shardSizes.get(shardId)).map { info =>
            val card: ReactElement = ShardInfoCard(info, state.advanced).withKey(s"shard-info-$shardId")
            shardId -> card
          }
        }

    div(
      style := js.Dynamic.literal(padding = "1em")
    )(
      h2(className := "px-3")("System Dashboard"),
      // aesthetic invariant: each `col` will have `mt-3`
      div(className := "dashboard grid px-3")(
        div(key := "header-row", className := "row")( // header row
          div(className := "col-12 mt-3")(
            s"Data accurate as of ${(state.metrics.atTime).atZone(ZoneId.of("GMT")).format(DateTimeFormatter.RFC_1123_DATE_TIME)}",
            div(className := "float-end")(
              label(
                "Advanced debugging: ",
                input(
                  `type` := "checkbox",
                  checked := state.advanced,
                  onChange := { event =>
                    val newCheckedState = event.currentTarget.checked
                    setState(_.copy(advanced = newCheckedState))
                  }
                )
              )
            )
          )
        ), // end header row
        div(key := "error-row", className := "row")( // error row
          state.errorMessage.toList.map(msg =>
            div(className := "col-12 mt-3", style := js.Dynamic.literal(color = "red"))(msg)
          ): _*
        ),
        div( // memory info row
          key := "memory-info-row",
          className := "row"
        )(
          extractMemoryCards().map { case (keyTitle: String, memoryCard: ReactElement) =>
            div(key := keyTitle, className := "col-12 col-md-6 col-xl-3 mt-3")(
              memoryCard
            )
          }
        ), // end memory info row
        div( // shard info row
          key := "shard-info-row",
          className := "row"
        )(
          shardInfos.map { case (shardId: Int, shardInfoCard: ReactElement) =>
            div(key := s"shard-$shardId", className := "col-12 col-md-6 col-xl-3 mt-3")(
              shardInfoCard
            )
          }
        ), // end shard info row
        div( // binary histograms row
          key := "binary-histograms-row",
          className := "row"
        )(
          histograms.map { case (keyTitle: String, histogramCard: ReactElement) =>
            div(
              key := keyTitle,
              className := "col-12 col-md-6 mt-3"
            )(histogramCard)
          }
        ), // end histograms row
        div( // timers row
          key := "timers-row",
          className := "row"
        )(
          timers.map { case (keyTitle: String, timerCard: ReactElement) =>
            div(
              key := keyTitle,
              className := "col-12 col-md-6 col-xl-3 mt-3"
            )(timerCard)
          }
        ) // end timers row
      )
    )
  }
}
