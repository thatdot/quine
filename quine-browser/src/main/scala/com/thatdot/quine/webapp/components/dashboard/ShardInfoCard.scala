package com.thatdot.quine.webapp.components.dashboard

import scala.util.matching.Regex

import com.raquo.laminar.api.L._

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
          sourceCounters.toString,
          limits.toString,
        )
        None
      }
    }
  }

  def apply(
    info: ShardInfo,
    displayAlerts: Boolean,
  ): HtmlElement = {
    val silencedAlertsVar = Var(
      info.alertCounters.map { case (alertName, _) =>
        alertName -> DefaultAlertThreshold
      },
    )

    def setAlertThreshold(alert: String, newThreshold: Long): Unit =
      silencedAlertsVar.update(_.updated(alert, newThreshold))

    def alerts(): HtmlElement =
      div(
        cls := "alerts",
        children <-- silencedAlertsVar.signal.map { silencedAlerts =>
          (for {
            (alert, count) <- info.alertCounters.toSeq
            threshold = silencedAlerts.getOrElse[Long](
              alert, {
                setAlertThreshold(alert, DefaultAlertThreshold)
                0
              },
            )
            if count > threshold
          } yield div(
            cls := "alert alert-warning",
            s"$alert is $count, exceeding threshold ($threshold)",
            button(
              cls := "close",
              onClick --> { _ => setAlertThreshold(alert, count) },
              span("\u00D7"),
            ),
          )): Seq[HtmlElement]
        },
      )

    def progressBar(): HtmlElement =
      ProgressBarMeter(
        name = "Nodes awake",
        value = info.nodesAwake.toDouble,
        softMax = info.nodeSoftMax.toDouble,
        hardMax = info.nodeHardMax.toDouble,
      )

    Card(
      title = s"Shard ${info.shardId}",
      body = if (displayAlerts) div(alerts(), progressBar()) else progressBar(),
    )
  }
}
