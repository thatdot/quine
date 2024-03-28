package com.thatdot.quine.app.routes.websocketquinepattern

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContextExecutor

import org.apache.pekko.actor.Actor
import org.apache.pekko.http.scaladsl.model.ws.{BinaryMessage, TextMessage}
import org.apache.pekko.stream.Materializer

import com.quine.language.server.QuineLanguageServer
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{Decoder, HCursor}
import org.eclipse.lsp4j.InitializeParams

object QuineLanguageServerActor {
  sealed trait ClientMessage
  final case object Initialize extends ClientMessage
  final case object UnsupportedTextMessageFromClient extends ClientMessage

  implicit val decodeClientMessage: Decoder[ClientMessage] = (c: HCursor) => {
    c.as[String].map {
      case "Initialize" =>
        Initialize
      case _ => UnsupportedTextMessageFromClient
    }
  }

  sealed trait ServerMessage
  final case object UnsupportedTextMessage extends ServerMessage
  final case object BinaryNotSupported extends ServerMessage
  final case object InitializeSuccess extends ServerMessage

  def serverMessageToTextMessage(serverMessage: ServerMessage): TextMessage.Strict = {
    val unsupportedTextMessage: TextMessage.Strict = TextMessage.Strict("UnsupportedTextMessage".asJson.noSpaces)
    val binaryNotSupported: TextMessage.Strict =
      TextMessage.Strict("BinaryNotSupported".asJson.noSpaces)
    val initializeSuccess: TextMessage.Strict = TextMessage.Strict("InitializeSuccess".asJson.noSpaces)

    serverMessage match {
      case InitializeSuccess => initializeSuccess
      case BinaryNotSupported => binaryNotSupported
      case UnsupportedTextMessage => unsupportedTextMessage
    }
  }
}

class QuineLanguageServerActor extends Actor {
  import QuineLanguageServerActor._

  implicit val materializer: Materializer = Materializer.matFromSystem(context.system)
  implicit val ec: ExecutionContextExecutor = context.system.dispatcher

  val quineLanguageServer = new QuineLanguageServer()

  def receive: Receive = {
    case tm: TextMessage =>
      val sender = context.sender()

      tm.textStream.runFold("")(_ + _).foreach { textString =>
        decode[ClientMessage](textString) match {
          case Right(Initialize) =>
            Option(quineLanguageServer.initialize(new InitializeParams())).map(_.toScala) match {
              case Some(_) => sender ! serverMessageToTextMessage(InitializeSuccess)

              // .initialize currently returns null, so we need to handle this case until it is updated
              case None => sender ! serverMessageToTextMessage(InitializeSuccess)
            }

          case _ =>
            sender ! serverMessageToTextMessage(UnsupportedTextMessage)
        }
      }
    case _: BinaryMessage => sender() ! serverMessageToTextMessage(BinaryNotSupported)
  }
}
