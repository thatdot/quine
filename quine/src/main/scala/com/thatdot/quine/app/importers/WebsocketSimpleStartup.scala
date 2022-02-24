package com.thatdot.quine.app.importers

import java.nio.charset.Charset

import scala.compat.ExecutionContexts
import scala.util.{Failure, Success}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.IllegalUriException
import akka.http.scaladsl.model.ws.{
  BinaryMessage,
  InvalidUpgradeResponse,
  Message,
  TextMessage,
  ValidUpgrade,
  WebSocketRequest
}
import akka.stream.KillSwitches
import akka.stream.contrib.{SwitchMode, Valve}
import akka.stream.scaladsl.{Flow, Keep, Source}
import akka.util.ByteString

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.ControlSwitches
import com.thatdot.quine.app.importers.WebsocketSimpleStartup.UpgradeFailedException
import com.thatdot.quine.app.importers.serialization.ImportFormat
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.graph.MasterStream.{IngestSrcExecToken, IngestSrcType}
object WebsocketSimpleStartup {
  class UpgradeFailedException(cause: Throwable)
      extends RuntimeException("Unable to upgrade to websocket connection", cause) {
    def this(cause: String) = this(new Throwable(cause))
  }
}
case class WebsocketSimpleStartup(
  format: ImportFormat,
  wsUrl: String,
  initMessages: Seq[String],
  parallelism: Int,
  charset: Charset,
  charsetTranscoder: Flow[ByteString, ByteString, NotUsed],
  meter: IngestMeter,
  initialSwitchMode: SwitchMode
) extends LazyLogging {
  @throws[IllegalUriException]("When the provided url is invalid")
  def stream(implicit graph: CypherOpsGraph): IngestSrcType[ControlSwitches] = {
    implicit val system: ActorSystem = graph.system
    // NB Instead of killing this source with the downstream KillSwitch, we could switch this Source.never to a
    // Source.maybe, completing it with None to kill the connection -- this is closer to the docs for
    // webSocketClientFlow
    val outboundMessages = Source.fromIterator(() => initMessages.iterator).map(TextMessage(_)).concat(Source.never)
    val wsFlow = Http().webSocketClientFlow(WebSocketRequest(wsUrl))
    val execToken = IngestSrcExecToken(s"WebSocket: $wsUrl")

    val (websocketUpgraded, websocketSource) = outboundMessages
      .viaMat(wsFlow)(Keep.right)
      .preMaterialize()

    Source
      .futureSource(websocketUpgraded.transform {
        // if the websocket upgrade fails, return an already-failed Source
        case Success(InvalidUpgradeResponse(_, cause)) => Failure(new UpgradeFailedException(cause))
        case Failure(ex) => Failure(new UpgradeFailedException(ex))
        // the websocket upgrade succeeded: proceed with setting up the ingest stream source
        case Success(ValidUpgrade(_, _)) =>
          Success(
            websocketSource
              .viaMat(KillSwitches.single)(Keep.right)
              .viaMat(Valve[Message](initialSwitchMode))(Keep.both)
              .flatMapConcat { msg =>
                val msgBytesSrc: Source[ByteString, _] =
                  msg match {
                    case textMessage: TextMessage =>
                      textMessage.textStream
                        .fold("")(_ + _)
                        .map(ByteString.fromString(_, charset))
                    case m: BinaryMessage =>
                      m.dataStream.fold(ByteString.empty)(_ concat _)
                  }
                msgBytesSrc.mapConcat { bytes =>
                  meter.mark(bytes.length)
                  format
                    .importMessageSafeBytes(bytes.toArray, graph.isSingleHost)
                    .fold(
                      { err =>
                        logger.warn(s"Error decoding event data for event $msg", err)
                        List.empty
                      },
                      List(_)
                    )
                }
              }
              .via(graph.ingestThrottleFlow)
              .mapAsyncUnordered(parallelism)(format.writeToGraph(graph, _))
              .map(_ => execToken)
              .watchTermination() { case ((killSwitch, valveSwitch), doneFut) =>
                valveSwitch.map(v => ControlSwitches(killSwitch, v, doneFut))(ExecutionContexts.parasitic)
              }
          )
      }(ExecutionContexts.parasitic))
      .mapMaterializedValue(_.flatten)

  }
}
