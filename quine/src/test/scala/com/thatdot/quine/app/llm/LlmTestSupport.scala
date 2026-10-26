package com.thatdot.quine.app.llm

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.Try

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{
  ContentTypes,
  HttpEntity,
  HttpHeader,
  HttpRequest,
  HttpResponse,
  StatusCode,
  StatusCodes,
}
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.util.ByteString

import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._
import org.scalatest.{BeforeAndAfterAll, Suite}

import com.thatdot.common.logging.Log.LogConfig

/** An actor system per suite, and a blocking `await` with one timeout for the whole suite. */
trait LlmTestSupport extends BeforeAndAfterAll { this: Suite =>
  implicit lazy val system: ActorSystem = ActorSystem(getClass.getSimpleName)
  implicit lazy val ec: ExecutionContext = system.dispatcher
  implicit val logConfig: LogConfig = LogConfig.permissive

  def await[A](future: Future[A]): A = Await.result(future, 10.seconds)

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 10.seconds)
    super.afterAll()
  }

  implicit class LoneElement[A](elements: Vector[A]) {
    def loneElement: A = { assert(elements.size == 1, s"expected exactly one element, got $elements"); elements.head }
  }
}

/** A model server below [[PostToLlmProfile]], run in-process as its [[LlmHttp.PoolFor]] so no port is bound. Each request
  * is captured and answered by `respond`; a `respond` that throws stands in for a transport failure.
  */
final class LlmStubServer(respond: LlmStubServer.Captured => HttpResponse)(implicit system: ActorSystem) {
  import LlmStubServer.Captured

  implicit private val ec: ExecutionContext = system.dispatcher
  private val captured = new AtomicReference(Vector.empty[Captured])

  def received: Vector[Captured] = captured.get()

  val pool: LlmHttp.PoolFor = (_, settings) =>
    Flow[(HttpRequest, Promise[LlmHttp.StrictResponse])].mapAsyncUnordered(settings.maxConnections) {
      case (request, promise) =>
        request.entity.dataBytes.runFold(ByteString.empty)(_ ++ _).map { bytes =>
          val body = parse(bytes.utf8String).getOrElse(Json.Null)
          val record = Captured(request.uri.path.toString, request.headers.toList, body)
          captured.updateAndGet(_ :+ record)
          (Try(respond(record)), promise)
        }
    }
}

object LlmStubServer {
  final case class Captured(path: String, headers: List[HttpHeader], body: Json)

  /** A pool that takes every call and answers none of them. */
  val never: LlmHttp.PoolFor = (_, _) =>
    Flow[(HttpRequest, Promise[LlmHttp.StrictResponse])].mapAsync(1)(_ => Future.never)

  /** A pool whose stream fails on the first call, as a bug in the pool would. */
  def failing(cause: Throwable): LlmHttp.PoolFor =
    (_, _) => Flow[(HttpRequest, Promise[LlmHttp.StrictResponse])].map(_ => throw cause)

  def json(body: Json, status: StatusCode = StatusCodes.OK): HttpResponse =
    HttpResponse(status, entity = HttpEntity(ContentTypes.`application/json`, body.noSpaces))

  def text(body: String, status: StatusCode): HttpResponse =
    HttpResponse(status, entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, body))

  def openAiCompletion(
    content: Option[String],
    finishReason: String = "stop",
    refusal: Option[String] = None,
  ): Json =
    Json.obj(
      "id" -> "chatcmpl-1".asJson,
      "choices" -> Json.arr(
        Json.obj(
          "index" -> 0.asJson,
          "finish_reason" -> finishReason.asJson,
          "message" -> Json.fromFields(
            List("role" -> "assistant".asJson, "content" -> content.asJson) ++ refusal.map("refusal" -> _.asJson),
          ),
        ),
      ),
      "usage" -> Json.obj("prompt_tokens" -> 12.asJson, "completion_tokens" -> 3.asJson),
    )

  def anthropicCompletion(texts: List[String], stopReason: String = "end_turn"): Json =
    Json.obj(
      "id" -> "msg_1".asJson,
      "type" -> "message".asJson,
      "role" -> "assistant".asJson,
      "content" -> Json.arr(texts.map(t => Json.obj("type" -> "text".asJson, "text" -> t.asJson)): _*),
      "stop_reason" -> stopReason.asJson,
      "usage" -> Json.obj("input_tokens" -> 20.asJson, "output_tokens" -> 4.asJson),
    )
}

/** Stands in for [[PostToLlmProfile]] above HTTP: captures each call and answers with `respond`, which may fail. */
final class LlmStubPost(respond: LlmStubPost.Posted => Future[Json]) {
  import LlmStubPost.Posted

  private val captured = new AtomicReference(Vector.empty[Posted])

  def received: Vector[Posted] = captured.get()

  val post: LlmHttp.Post = { (uri, headers, body) =>
    val record = Posted(uri, headers, body)
    captured.updateAndGet(_ :+ record)
    respond(record)
  }
}

object LlmStubPost {
  final case class Posted(uri: String, headers: List[HttpHeader], body: Json)

  /** A stub that answers every call with `body`. */
  def answering(body: Json): LlmStubPost = new LlmStubPost(_ => Future.successful(body))
}
