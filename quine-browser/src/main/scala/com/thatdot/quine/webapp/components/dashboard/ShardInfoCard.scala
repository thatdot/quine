package com.thatdot.quine.webapp.components.dashboard

import scala.util.matching.Regex

import slinky.core.Component
import slinky.core.annotations.react
import slinky.core.facade.ReactElement
import slinky.web.html._

import com.thatdot.quine.routes.{Counter, ShardInMemoryLimit}

object ShardInfoCard {
  val DefaultAlertThreshold = 0L

  val ShardCounterName = new Regex(
    raw"shard\.(?:current-)?shard-(\d+)\.((?:unlikely\.(.*))|.*)",
    "shardId",
    "counterName",
    "alertName",
  )

  final case class ShardInfo(
    shardId: Int,
    nodeSoftMax: Int,
    nodeHardMax: Int,
    nodesWoken: Long,
    nodesSleepFailure: Long,
    nodesSleepSuccess: Long,
    nodesRemoved: Long,
    alertCounters: Map[String, Long],
  ) {
    def nodesAwake: Long = nodesWoken - (nodesSleepFailure + nodesSleepSuccess + nodesRemoved)
  }
  object ShardInfo {
    def forShard(
      shardId: Int,
      sourceCounters: Seq[Counter],
      limits: Option[ShardInMemoryLimit],
    ): Option[ShardInfo] = {
      val countsForThisShard = sourceCounters.collect {
        case Counter(ShardCounterName(countsForShardId, counterName, _), count) if shardId == countsForShardId.toInt =>
          (counterName -> count)
      }.toMap

      val alertCounters = sourceCounters.collect {
        case Counter(ShardCounterName(countsForShardId, _, alertName), count)
            if shardId == countsForShardId.toInt && alertName != null && alertName.nonEmpty =>
          (alertName -> count)
      }.toMap

      // flatmap a ton of Options together for input validation. This should succeed any time after initialization.
      (for {
        nodesWoken <- countsForThisShard.get("sleep-counters.woken")
        nodesSleepFailure <- countsForThisShard.get("sleep-counters.slept-failure")
        nodesSleepSuccess <- countsForThisShard.get("sleep-counters.slept-success")
        nodesRemoved <- countsForThisShard.get("sleep-counters.removed")
        ShardInMemoryLimit(nodeSoftMax, nodeHardMax) <- limits
      } yield ShardInfo(
        shardId,
        nodeSoftMax,
        nodeHardMax,
        nodesWoken,
        nodesSleepFailure,
        nodesSleepSuccess,
        nodesRemoved,
        alertCounters,
      )).orElse {
        org.scalajs.dom.console.warn(
          s"Unable to find all necessary information to populate card for shard: $shardId. Logging sourceCounters and shard limit",
          sourceCounters,
          limits,
        )
        None
      }
    }
  }

}
@react class ShardInfoCard extends Component {

  /** @param silencedAlerts counter values at or below which alerts for named counters should be suppressed
    */
  case class State(silencedAlerts: Map[String, Long])
  case class Props(
    info: ShardInfoCard.ShardInfo,
    displayAlerts: Boolean,
  )
  // The initial state sets a silencing threshold for all known alerts to 0, where "known" means "name was provided as part of props"
  override def initialState: State = {
    val silencedAlerts = props.info.alertCounters.map { case (alertName, _) =>
      alertName -> ShardInfoCard.DefaultAlertThreshold
    }
    State(silencedAlerts = silencedAlerts)
  }

  private def setAlertThreshold(alert: String, newThreshold: Long): Unit =
    setState(s => s.copy(silencedAlerts = s.silencedAlerts.updated(alert, newThreshold)))

  /** Calculate alerts that are above their silence threshold and register previously-unseen alerts with a threshold of 0
    * @return
    */
  def alerts(): ReactElement =
    div(className := "alerts", key := "alerts")(
      for {
        (alert, count) <- props.info.alertCounters: Map[String, Long]
        threshold = state.silencedAlerts
          .getOrElse[Long](
            alert, { // if the alert is a new one, register it with a threshold of 0
              setAlertThreshold(alert, ShardInfoCard.DefaultAlertThreshold)
              0
            },
          )
        if count > threshold // filter to only alerts above their silencing threshold
      } yield div(className := "alert alert-warning", key := alert)(
        s"$alert is $count, exceeding threshold ($threshold)",
        button(className := "close", onClick := (() => setAlertThreshold(alert, count)))(
          span("\u00D7"), // HTML entity &times;
        ),
      ),
    )

  def progressBar(): ReactElement =
    ProgressBarMeter(
      name = "Nodes awake",
      value = props.info.nodesAwake.toDouble,
      softMax = props.info.nodeSoftMax.toDouble,
      hardMax = props.info.nodeHardMax.toDouble,
    )

  override def render(): ReactElement =
    Card(
      title = s"Shard ${props.info.shardId}",
      body = if (props.displayAlerts) div(alerts(), progressBar()) else progressBar(),
    )
}
