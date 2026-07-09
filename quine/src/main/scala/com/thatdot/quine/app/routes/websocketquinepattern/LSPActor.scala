package com.thatdot.quine.app.routes.websocketquinepattern

import java.io.{OutputStream, PipedInputStream, PipedOutputStream}
import java.util.concurrent.{ExecutorService, Executors}

import scala.concurrent.ExecutionContext

import org.apache.pekko.actor.{Actor, ActorRef, Stash, Status}
import org.apache.pekko.http.scaladsl.model.ws.{BinaryMessage, Message => WsMessage, TextMessage}
import org.apache.pekko.pattern.pipe
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.ByteString

import io.circe.parser._
import org.eclipse.lsp4j.jsonrpc.messages.Message
import org.eclipse.lsp4j.jsonrpc.{Launcher, MessageConsumer}
import org.eclipse.lsp4j.services.LanguageClient

import com.thatdot.common.logging.Log._
import com.thatdot.quine.language.server.QuineLanguageServer

/** Receives WebSocket Messages, pipes them to the LSPLauncher containing our `QuineLanguageServer`,
  * and sends the response to the `outgoingActorRef`, which sends the message to the client.
  *
  * @param outgoingActorRef Actor reference used to send messages back to the WebSocket client.
  */
object LSPActor {
  def apply(outgoingActorRef: ActorRef): LSPActor = new LSPActor(outgoingActorRef)
}

class LSPActor(outgoingActorRef: ActorRef) extends Actor with Stash with LazySafeLogging {
  implicit val ec: ExecutionContext = context.system.dispatcher
  implicit val materializer: Materializer = Materializer.matFromSystem(context.system)
  implicit val logConfig: LogConfig = LogConfig()
  implicit val throwableLogger: Loggable[Throwable] = toStringLoggable[Throwable]

  // Piped streams to connect Message streams w/ LSPLauncher
  val outClient = new PipedOutputStream() // from language client
  val inServer = new PipedInputStream() // to language server

  outClient.connect(inServer)

  // Back completions with the running graph's live function/procedure registries, so user-defined
  // functions and procedures are offered as completions.
  val server = new QuineLanguageServer(LiveNameCompletionSource)

  val messageWrapper: java.util.function.Function[MessageConsumer, MessageConsumer] =
    new java.util.function.Function[MessageConsumer, MessageConsumer] {
      def apply(consumer: MessageConsumer): MessageConsumer = new MessageConsumer {
        def consume(message: Message): Unit = {
          val messageString = message.toString()
          if (isOutgoingMessage(messageString)) {
            logger.trace(log"Message received from Quine Language Server, going to client: ${Safe(messageString)}")
            outgoingActorRef ! TextMessage.Strict(messageString)
          }
          consumer.consume(message)
        }
      }
    }
  // Own the thread pool lsp4j reads on; the default is an un-owned cached pool nothing shuts down.
  // Shutting this down in postStop reclaims the reader thread parked in PipedInputStream.read.
  val lspExecutor: ExecutorService = Executors.newCachedThreadPool()
  val launcher: Launcher[LanguageClient] = new Launcher.Builder[LanguageClient]()
    .setLocalService(server)
    .setRemoteInterface(classOf[LanguageClient])
    .setInput(inServer)
    .setOutput(OutputStream.nullOutputStream())
    .setExecutorService(lspExecutor)
    .wrapMessages(messageWrapper)
    .create()
  val clientProxy: LanguageClient = launcher.getRemoteProxy()
  server.connect(clientProxy)

  val listening: java.util.concurrent.Future[Void] = launcher.startListening()

  /** Wraps a folded streamed frame piped back to this actor, so [[folding]] can tell the fold
    * result apart from a genuine client frame that arrives (and must be stashed) while the fold is
    * still in flight — the two are otherwise both plain strict messages.
    */
  private case class FoldComplete(message: WsMessage)

  def receive: Receive = ready

  /** Default behavior: each strict frame is one complete JSON-RPC message, processed in arrival
    * order. A streamed frame must be folded into its full text before it can be processed, and that
    * fold is asynchronous — so while it is in flight this switches to [[folding]] and stashes every
    * later frame, guaranteeing the folded frame is processed before them. Without this a later
    * strict frame overtakes the still-folding streamed one and an out-of-order didChange corrupts
    * the server-side document buffer.
    */
  private def ready: Receive = {
    case TextMessage.Strict(text) =>
      processTextMessage(text)

    case BinaryMessage.Strict(data) =>
      processBinaryMessageBytes(data)

    case TextMessage.Streamed(textStream) =>
      textStream.runFold("")(_ + _).map(text => FoldComplete(TextMessage.Strict(text))).pipeTo(self)
      context.become(folding)

    case BinaryMessage.Streamed(dataStream) =>
      dataStream.runFold(ByteString.empty)(_ ++ _).map(data => FoldComplete(BinaryMessage.Strict(data))).pipeTo(self)
      context.become(folding)

    // A streamed-frame fold can fail; `pipeTo self` surfaces that as a Status.Failure. (Stream
    // completion is handled by the flow stopping this actor, not via the mailbox.)
    case Status.Failure(exception) =>
      logger.debug(safe"Streamed-frame fold failed: ${Safe(exception)}")

    case other =>
      logger.warn(safe"Received unexpected message: ${Safe(other.toString())}")
  }

  /** Active while a single streamed frame is folding: the folded frame is processed the instant it
    * arrives (and unstashing restores the frames that arrived meanwhile to their original order), a
    * fold failure is logged, and every other frame is stashed until the fold settles. Only one fold
    * is ever in flight, because further streamed frames are stashed here too.
    */
  private def folding: Receive = {
    case FoldComplete(message) =>
      message match {
        case TextMessage.Strict(text) => processTextMessage(text)
        case BinaryMessage.Strict(data) => processBinaryMessageBytes(data)
        case _ => () // runFold only ever yields the strict messages constructed in `ready`
      }
      unstashAll()
      context.become(ready)

    case Status.Failure(exception) =>
      logger.debug(safe"Streamed-frame fold failed: ${Safe(exception)}")
      unstashAll()
      context.become(ready)

    case _ =>
      stash()
  }

  def processTextMessage(text: String): Unit = {
    logger.trace(log"Message received from client, going to Quine Language Server: ${Safe(text)}")
    writeFramed(text.getBytes("UTF-8"))
  }

  def processBinaryMessageBytes(data: ByteString): Unit = {
    logger.trace(log"Binary message received from client (length: ${Safe(data.length.toString)} bytes)")
    writeFramed(data.toArray)
  }

  /** Writes one JSON-RPC message to the language server's input stream with the LSP `Content-Length`
    * header lsp4j's `StreamMessageProducer` requires (the UTF-8 byte length, then a blank line).
    * A text or binary frame each carries one complete message, so both are framed identically;
    * writing a binary frame's bytes unframed would desync the launcher's stream.
    */
  private def writeFramed(contentBytes: Array[Byte]): Unit = {
    val header = s"Content-Length: ${contentBytes.length}\r\n\r\n"
    outClient.write(header.getBytes("UTF-8"))
    outClient.write(contentBytes)
    outClient.flush()
  }

  override def postStop(): Unit = {
    // Cancel the listener (interrupting the parked read), then close the pipes so the read unblocks,
    // then shut down the pool so its reader thread is reclaimed rather than lingering.
    listening.cancel(true)
    outClient.close()
    inServer.close()
    lspExecutor.shutdownNow()
    super.postStop()
  }

  /** Decides whether a message emitted by the language server is forwarded to the WebSocket
    * client. JSON-RPC responses are forwarded — a success response carries a `result` member
    * and an error response carries `id` and `error` (per JSON-RPC 2.0 a response has exactly
    * one of the two) — so every client request settles, loudly, even when the server fails it.
    * Server-push messages (notifications and server-to-client requests, both identified by a
    * `method` member and the absence of `result`/`error`) are deliberately not forwarded.
    */
  def isOutgoingMessage(json: String): Boolean =
    parse(json) match {
      case Right(jsonObject) =>
        val cursor = jsonObject.hcursor
        val isSuccessResponse = cursor.downField("result").focus.isDefined
        val isErrorResponse =
          cursor.downField("id").focus.isDefined && cursor.downField("error").focus.isDefined
        isSuccessResponse || isErrorResponse
      case Left(error) =>
        // Not valid JSON: the language server should only ever emit JSON-RPC, so dropping this
        // silently would mask a server bug. Log at debug (the filter still fails closed).
        logger.debug(log"Dropping unparseable message from language server: ${Safe(error: Throwable)}")
        false
    }
}
