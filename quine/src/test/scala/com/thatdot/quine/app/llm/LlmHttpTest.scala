package com.thatdot.quine.app.llm

import java.net.ConnectException
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}
import scala.util.Try

import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, HttpResponse, StatusCodes}
import org.apache.pekko.pattern.after
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.util.ByteString
import org.apache.pekko.{Done, NotUsed}

import com.codahale.metrics.MetricRegistry
import io.circe.Json
import io.circe.syntax._
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.app.config.{LlmApi, LlmProfile}
import com.thatdot.quine.app.llm.LlmStubServer.{json, text}

/** [[PostToLlmProfile]]: the timeout, the retry, and the mapping of each failure to a [[LlmError]]. */
class LlmHttpTest extends AnyFunSuite with LlmTestSupport {

  private val profile = LlmProfile.Completion(
    api = LlmApi.OpenAiChat(),
    baseUrl = "http://model.test/v1",
    model = "m",
    maxConcurrentRequests = 3,
    requestTimeout = 20.seconds,
  )
  private val quick = profile.copy(requestTimeout = 200.millis)
  private val uri = "http://model.test/v1/chat/completions"
  private val body = Json.obj("q" -> "hi".asJson)
  private val ok = Json.obj("ok" -> Json.True)

  /** A `post` for `profile` whose retries wait a millisecond, so the retry cases run at test speed. */
  private def post(
    poolFor: LlmHttp.PoolFor,
    metrics: MetricRegistry = new MetricRegistry,
    profile: LlmProfile = profile,
    maxResponseBytes: Long = LlmHttp.maxResponseBytes,
  ): LlmHttp.Post =
    new PostToLlmProfile(
      "test-profile",
      profile,
      poolFor,
      metrics,
      minBackoff = 1.milli,
      maxResponseBytes = maxResponseBytes,
    )

  /** A response whose body streams the given chunks rather than arriving whole, as every real pool response does. */
  private def streamed(chunks: Source[ByteString, Any]): HttpResponse =
    HttpResponse(StatusCodes.OK, entity = HttpEntity(ContentTypes.`application/json`, chunks))

  private def meter(metrics: MetricRegistry, suffix: String): Long =
    metrics.meter(s"llm.test-profile.$suffix").getCount

  /** `chunks` plus a promise settled when the source terminates, so a test can prove a read was stopped. */
  private def watched(chunks: Source[ByteString, Any]): (Source[ByteString, Any], Promise[Done]) = {
    val terminated = Promise[Done]()
    val source = chunks.watchTermination() { (_, done) =>
      terminated.completeWith(done)
      NotUsed
    }
    (source, terminated)
  }

  /** Awaits the source's end, which the abort may deliver as a failure: either way it terminated. */
  private def assertTerminates(terminated: Promise[Done]): Unit = {
    await(terminated.future.recover { case _ => Done })
    ()
  }

  test("a call that outlives the request timeout fails with the timeout message and is not retried") {
    val metrics = new MetricRegistry
    val error = intercept[LlmError](await(post(LlmStubServer.never, metrics, profile = quick)(uri, Nil, body)))
    assert(error.getMessage == "the model endpoint did not answer within 200 milliseconds")
    assert(!error.retryable)
    assert(error.cannotServe, "a timeout is not retried, but a fallback profile may serve the call")
    assert(error.fault == LlmError.Fault.Unavailable)
    assert(error.status.isEmpty, "no response arrived, so there is no status")
    assert(meter(metrics, "attempts") == 1)
  }

  test("429 then 503 then 200 succeeds on the third attempt; 400 fails on the first") {
    val metrics = new MetricRegistry
    val calls = new AtomicInteger(0)
    val flaky = new LlmStubServer(_ =>
      calls.incrementAndGet() match {
        case 1 => text("slow down", StatusCodes.TooManyRequests)
        case 2 => text("still down", StatusCodes.ServiceUnavailable)
        case _ => json(ok)
      },
    )
    assert(await(post(flaky.pool, metrics)(uri, Nil, body)) == ok)
    assert(calls.get() == 3)
    assert(meter(metrics, "attempts") == 3)

    val bad = new LlmStubServer(_ => text("bad schema", StatusCodes.BadRequest))
    val error = intercept[LlmError](await(post(bad.pool, metrics)(uri, Nil, body)))
    assert(error.getMessage == "the model endpoint returned status 400: bad schema")
    assert(!error.retryable)
    assert(!error.cannotServe, "the endpoint answered 400, so no fallback profile changes the outcome")
    assert(error.fault == LlmError.Fault.BadResponse, "the service rejected the request, an operator must act")
    assert(error.status.contains(400), "the status rides as a field, not only as message text")
    assert(bad.received.size == 1)
  }

  test("a transport failure is retried, and its message keeps the cause's class but not its text") {
    val refusing = new LlmStubServer(_ => throw new ConnectException("Connection refused: model.test/10.0.0.1:80"))
    val error = intercept[LlmError](await(post(refusing.pool)(uri, Nil, body)))
    assert(error.getMessage == "could not reach the model endpoint (ConnectException)")
    assert(error.retryable)
    assert(error.cannotServe)
    assert(error.fault == LlmError.Fault.Unavailable)
    assert(error.cause.exists(_.isInstanceOf[ConnectException]), "the exception rides as the cause, for operator logs")
    assert(error.getCause.getMessage.contains("model.test"), "the cause keeps the host the message may not")
    assert(refusing.received.size == 3, "three attempts for the refused call")
  }

  test("a body that stalls after the headers fails with the timeout message: the deadline covers the body") {
    val metrics = new MetricRegistry
    val (chunks, terminated) = watched(Source.single(ByteString("{")).concat(Source.never))
    val stalling = new LlmStubServer(_ => streamed(chunks))
    val error = intercept[LlmError](await(post(stalling.pool, metrics, profile = quick)(uri, Nil, body)))
    assert(error.getMessage == "the model endpoint did not answer within 200 milliseconds")
    assert(!error.retryable)
    assert(error.cannotServe)
    assert(error.fault == LlmError.Fault.Unavailable)
    assert(meter(metrics, "attempts") == 1)
    // Settling the promise aborts the read: without the abort this await fails the test by timing out
    assertTerminates(terminated)
  }

  test("a response that arrives after the timeout is cancelled, not drained") {
    val (chunks, terminated) = watched(Source.single(ByteString("{")).concat(Source.never))
    // A pool that answers past `quick`'s 200 millisecond timeout, with a body that never ends
    val late: LlmHttp.PoolFor = (_, _) =>
      Flow[(HttpRequest, Promise[LlmHttp.StrictResponse])].mapAsync(1) { case (_, promise) =>
        after(400.millis)(Future.successful((Try(streamed(chunks)): Try[HttpResponse], promise)))
      }
    val error = intercept[LlmError](await(post(late, profile = quick)(uri, Nil, body)))
    assert(error.getMessage == "the model endpoint did not answer within 200 milliseconds")
    // Draining instead of cancelling would hold this body, and its connection, forever
    assertTerminates(terminated)
  }

  test("a connection that fails while the body streams is classified, keeps its cause, and is retried") {
    val reset = new RuntimeException("Connection reset by model.test/10.0.0.1:80")
    val truncating = new LlmStubServer(_ => streamed(Source.single(ByteString("{\"ok\"")).concat(Source.failed(reset))))
    val error = intercept[LlmError](await(post(truncating.pool)(uri, Nil, body)))
    assert(
      error.getMessage == "the connection to the model endpoint failed before the full response arrived (RuntimeException)",
    )
    assert(error.retryable)
    assert(error.cannotServe)
    assert(error.fault == LlmError.Fault.Unavailable)
    assert(error.cause.contains(reset), "the exception rides as the cause, for operator logs")
    assert(truncating.received.size == 3, "three attempts for the truncated body")
  }

  test("a body over the ceiling fails at once, names the ceiling and not the body, and is not retried") {
    val metrics = new MetricRegistry
    val oversized = new LlmStubServer(_ => streamed(Source.single(ByteString("\"" + "x" * 100 + "\""))))
    val error = intercept[LlmError](await(post(oversized.pool, metrics, maxResponseBytes = 64)(uri, Nil, body)))
    assert(error.getMessage == "the model endpoint sent a response over 64 bytes")
    assert(!error.retryable)
    assert(!error.cannotServe, "an oversized answer is an answer, so no fallback profile changes the outcome")
    assert(error.fault == LlmError.Fault.BadResponse)
    assert(error.status.contains(200), "the status arrived, so it rides as a field")
    assert(meter(metrics, "attempts") == 1)
  }

  test("a pool that stops leaves the call in flight to the timeout and refuses every later call") {
    val send = post(LlmStubServer.failing(new IllegalStateException("boom")), profile = quick)
    val first = intercept[LlmError](await(send(uri, Nil, body)))
    assert(first.getMessage == "the model endpoint did not answer within 200 milliseconds")
    val second = intercept[LlmError](await(send(uri, Nil, body)))
    assert(second.getMessage == """profile "test-profile" no longer accepts requests, its pool has stopped""")
    assert(!second.retryable)
    assert(second.cannotServe, "a stopped pool means the profile cannot serve the call")
    assert(
      second.fault == LlmError.Fault.BadResponse,
      "the pool is never rebuilt, so only an operator restart clears this",
    )
  }
}
