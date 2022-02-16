package com.thatdot.quine.app

import scala.compat.ExecutionContexts
import scala.concurrent.Future

import akka.Done
import akka.stream.UniqueKillSwitch
import akka.stream.contrib.{SwitchMode, ValveSwitch}

import com.thatdot.quine.graph.IngestControl

sealed trait QuineAppIngestControl extends IngestControl {
  val valveHandle: ValveSwitch
  val termSignal: Future[Done]
}

final case class ControlSwitches(killSwitch: UniqueKillSwitch, valveHandle: ValveSwitch, termSignal: Future[Done])
    extends QuineAppIngestControl {
  def pause(): Future[Boolean] = valveHandle.flip(SwitchMode.Close)
  def unpause(): Future[Boolean] = valveHandle.flip(SwitchMode.Open)
  def terminate(): Future[Done] = {
    killSwitch.shutdown()
    termSignal
  }
}

final class KafkaControl(
  killSwitch: akka.kafka.scaladsl.Consumer.Control,
  val valveHandle: ValveSwitch,
  val termSignal: Future[akka.Done]
) extends QuineAppIngestControl {
  def pause(): Future[Boolean] = valveHandle.flip(SwitchMode.Close)
  def unpause(): Future[Boolean] = valveHandle.flip(SwitchMode.Open)
  // See https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#controlled-shutdown
  override def terminate(): Future[akka.Done] = killSwitch.drainAndShutdown(termSignal)(ExecutionContexts.parasitic)
}
