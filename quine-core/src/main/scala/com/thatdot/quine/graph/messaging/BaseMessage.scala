package com.thatdot.quine.graph.messaging

import scala.concurrent.duration.{DurationDouble, DurationLong, FiniteDuration}

import org.apache.pekko.actor.ActorRef

/** Messages that are re-used in the most basic protocols in Quine */
sealed abstract class BaseMessage extends QuineMessage

object BaseMessage {

  final case class Response(response: QuineResponse) extends BaseMessage

  final case class DeliveryRelay(msg: QuineMessage, dedupId: Long, needsAck: Boolean) extends BaseMessage

  /** Sent to/from exactly once actors to confirm cross-host message delivery
    *
    * @note since exactly once actors expect this to have a special meaning, it
    *       is important *not* to re-use this message to mean anything else
    */
  case object Ack extends BaseMessage

  case object Done extends BaseMessage

  object LocalMessageDelivery {
    val remainingRetriesMax: Int = 3000
    val singleLocalDeliveryMaxDelay: FiniteDuration = 0.2.seconds
    val beginDelayingThreshold: Int = remainingRetriesMax / 4
    require(remainingRetriesMax > beginDelayingThreshold)
    require(beginDelayingThreshold > 0)

    // Linearly increasing delay starting after `beginDelayingThreshold`, then ascending from 0 to
    // `singleLocalDeliveryMaxDelay` as `remainingRetriesMax` approaches 0:  (shaped like ReLU)
    def slidingDelay(remaining: Int): Option[FiniteDuration] =
      if (remaining >= beginDelayingThreshold) None
      else
        Some(
          singleLocalDeliveryMaxDelay * ((beginDelayingThreshold - remaining).toDouble / beginDelayingThreshold)
        ).map(d => d.toMillis.millis)
  }

  /** Used to deliver messages to individual nodes from shards
    */
  final case class LocalMessageDelivery(
    msg: QuineMessage,
    targetQid: QuineIdAtTime,
    originalSender: ActorRef
  ) extends BaseMessage

}
