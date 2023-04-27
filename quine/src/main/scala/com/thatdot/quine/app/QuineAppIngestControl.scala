package com.thatdot.quine.app

import scala.compat.ExecutionContexts
import scala.concurrent.{ExecutionContext, Future}

import akka.Done
import akka.stream.UniqueKillSwitch

import com.thatdot.quine.graph.IngestControl
import com.thatdot.quine.util.{SwitchMode, ValveSwitch}

sealed trait QuineAppIngestControl extends IngestControl {
  val valveHandle: ValveSwitch
  val termSignal: Future[Done]
  def pause(): Future[Boolean]
  def unpause(): Future[Boolean]
  def terminate(): Future[Done]
}

final case class ControlSwitches(shutdownSwitch: ShutdownSwitch, valveHandle: ValveSwitch, termSignal: Future[Done])
    extends QuineAppIngestControl {
  def pause(): Future[Boolean] = valveHandle.flip(SwitchMode.Close)
  def unpause(): Future[Boolean] = valveHandle.flip(SwitchMode.Open)
  def terminate(): Future[Done] = shutdownSwitch.terminate(termSignal)
}

/** This allows us to generalize over ingests where we're manually adding akka stream kill switches and libraries
  * (such as kafka) that provide a stream with a library class wrapping a kill switch.
  */
trait ShutdownSwitch {
  def terminate(termSignal: Future[akka.Done]): Future[Done]
}

case class AkkaKillSwitch(killSwitch: UniqueKillSwitch) extends ShutdownSwitch {
  def terminate(termSignal: Future[akka.Done]): Future[Done] = {
    killSwitch.shutdown()
    termSignal
  }
}
case class KafkaKillSwitch(killSwitch: akka.kafka.scaladsl.Consumer.Control) extends ShutdownSwitch {
  def terminate(termSignal: Future[akka.Done]): Future[akka.Done] =
    killSwitch.drainAndShutdown(termSignal)(ExecutionContexts.parasitic)
}

case class PulsarKillSwitch(control: com.sksamuel.pulsar4s.akka.streams.Control, ctx: ExecutionContext)
    extends ShutdownSwitch {
  def terminate(termSignal: Future[akka.Done]): Future[Done] =
    termSignal.flatMap(_ => control.shutdown()(ctx))(ExecutionContexts.parasitic)

}
