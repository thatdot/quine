package com.thatdot.quine.app.routes.websocketquinepattern

import java.io.{OutputStream, PipedInputStream, PipedOutputStream}

import scala.concurrent.ExecutionContext

import org.apache.pekko.actor.{Actor, ActorRef, Status}
import org.apache.pekko.http.scaladsl.model.ws.TextMessage
import org.apache.pekko.pattern.pipe
import org.apache.pekko.stream.Materializer

import io.circe.parser._
import org.eclipse.lsp4j.jsonrpc.messages.Message
import org.eclipse.lsp4j.jsonrpc.{Launcher, MessageConsumer}
import org.eclipse.lsp4j.services.LanguageClient

import com.thatdot.language.server.QuineLanguageServer
import com.thatdot.quine.util.Log._

/** Receives WebSocket Messages, pipes them to the LSPLauncher containing our `QuineLanguageServer`,
  * and sends the response to the `outgoingActorRef`, which sends the message to the client.
  *
  * @param outgoingActorRef Actor reference used to send messages back to the WebSocket client.
  */
object LSPActor {
  def apply(outgoingActorRef: ActorRef): LSPActor = new LSPActor(outgoingActorRef)
}

class LSPActor(outgoingActorRef: ActorRef) extends Actor with LazySafeLogging {
  implicit val ec: ExecutionContext = context.system.dispatcher
  implicit val materializer: Materializer = Materializer.matFromSystem(context.system)
  implicit val logConfig: LogConfig = LogConfig()
  implicit val throwableLogger: Loggable[Throwable] = toStringLoggable[Throwable]

  // Piped streams to connect Message streams w/ LSPLauncher
  val outClient = new PipedOutputStream() // from language client
  val inServer = new PipedInputStream() // to language server

  outClient.connect(inServer)

  val server = new QuineLanguageServer()

  val messageWrapper: java.util.function.Function[MessageConsumer, MessageConsumer] =
    new java.util.function.Function[MessageConsumer, MessageConsumer] {
      def apply(consumer: MessageConsumer): MessageConsumer = new MessageConsumer {
        def consume(message: Message): Unit = {
          val messageString = message.toString()
          if (isOutgoingMessage(messageString)) {
            logger.info(log"Message received from Quine Language Server, going to client: ${Safe(messageString)}")
            outgoingActorRef ! TextMessage.Strict(messageString)
          }
          consumer.consume(message)
        }
      }
    }
  val launcher: Launcher[LanguageClient] = new Launcher.Builder[LanguageClient]()
    .setLocalService(server)
    .setRemoteInterface(classOf[LanguageClient])
    .setInput(inServer)
    .setOutput(OutputStream.nullOutputStream())
    .wrapMessages(messageWrapper)
    .create()
  val clientProxy: LanguageClient = launcher.getRemoteProxy()
  server.connect(clientProxy)

  val listening: java.util.concurrent.Future[Void] = launcher.startListening()

  def receive: Receive = {
    case TextMessage.Strict(text) =>
      processTextMessage(text)

    case TextMessage.Streamed(textStream) =>
      textStream.runFold("")(_ + _).map(TextMessage.Strict).pipeTo(self)
      ()

    case Status.Success(_) =>
      logger.info(log"Stream completed")

    case Status.Failure(exception) =>
      logger.info(safe"Stream failed with exception: ${Safe(exception)}")

    case other =>
      logger.info(safe"Received unexpected message: ${Safe(other.toString())}")
  }

  def processTextMessage(text: String): Unit = {
    logger.info(log"Message received from client, going to Quine Language Server: ${Safe(text)}")
    val contentBytes = text.getBytes("UTF-8")
    val header = s"Content-Length: ${contentBytes.length}\r\n\r\n"
    outClient.write(header.getBytes("UTF-8"))
    outClient.write(contentBytes)
    outClient.flush()
  }

  override def postStop(): Unit = {
    outClient.close()
    inServer.close()
    listening.cancel(true)
    super.postStop()
  }

  def isOutgoingMessage(json: String): Boolean =
    parse(json) match {
      case Right(jsonObject) =>
        jsonObject.hcursor.downField("result").focus.isDefined
      case Left(_) =>
        false
    }
}
