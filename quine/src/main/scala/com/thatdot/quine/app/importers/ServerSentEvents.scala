package com.thatdot.quine.app.importers

import scala.compat.ExecutionContexts
import scala.concurrent.duration.DurationInt

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.model.{IllegalUriException, Uri}
import akka.stream.KillSwitches
import akka.stream.alpakka.sse.scaladsl.EventSource
import akka.stream.contrib.{SwitchMode, Valve}
import akka.stream.scaladsl.{Flow, Keep}

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.ControlSwitches
import com.thatdot.quine.app.importers.serialization.ImportFormat
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.MasterStream.{IngestSrcExecToken, IngestSrcType}

final case class ServerSentEvents(
  url: String,
  format: ImportFormat,
  meter: IngestMeter,
  initialSwitchMode: SwitchMode,
  parallelism: Int,
  maxPerSecond: Option[Int]
) extends LazyLogging {

  @throws[IllegalUriException]("When the provided url is invalid")
  def stream(implicit graph: CypherOpsGraph): IngestSrcType[ControlSwitches] = {
    implicit val system: ActorSystem = graph.system

    val isSingleHost = graph.isSingleHost
    val execToken = IngestSrcExecToken(s"SSE: $url")

    def throttled[A] = maxPerSecond match {
      case None => Flow[A]
      case Some(perSec) => Flow[A].throttle(perSec, 1.second)
    }

    EventSource(uri = Uri(url), send = Http().singleRequest(_))
      .viaMat(KillSwitches.single)(Keep.right)
      .viaMat(Valve[ServerSentEvent](initialSwitchMode))(Keep.both)
      .mapConcat { event =>
        val bytes = event.data.getBytes
        meter.mark(bytes.length)
        format
          .importMessageSafeBytes(bytes, isSingleHost)
          .fold(
            { err =>
              logger.warn(s"Error decoding event data for event $event", err)
              List.empty
            },
            List(_)
          )
      }
      .via(throttled)
      .via(graph.ingestThrottleFlow)
      .mapAsyncUnordered(parallelism)(format.writeToGraph(graph, _))
      .map(_ => execToken)
      .watchTermination() { case ((a, b), c) => b.map(v => ControlSwitches(a, v, c))(ExecutionContexts.parasitic) }
  }
}
