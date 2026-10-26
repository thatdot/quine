package com.thatdot.quine.app.llm

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.{NoStackTrace, NonFatal}
import scala.util.{Failure, Success, Try}

import org.apache.pekko.actor.{ActorSystem, Scheduler}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.headers.`User-Agent`
import org.apache.pekko.http.scaladsl.model.{
  ContentTypes,
  HttpEntity,
  HttpHeader,
  HttpMethods,
  HttpRequest,
  HttpResponse,
  StatusCode,
  Uri,
}
import org.apache.pekko.http.scaladsl.settings.{ClientConnectionSettings, ConnectionPoolSettings}
import org.apache.pekko.pattern.retry
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.stream.{KillSwitches, QueueCompletionResult, QueueOfferResult, StreamLimitReachedException}
import org.apache.pekko.util.ByteString

import com.codahale.metrics.MetricRegistry
import io.circe.Json
import io.circe.parser.parse

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.app.config.LlmProfile
import com.thatdot.quine.graph.metrics.HostQuineMetrics.LlmMetricComponent

/** The one HTTP exchange every model api performs: POST a JSON body, get JSON back. */
object LlmHttp {

  /** A response read in full: its status and every body byte. The pool answers at the headers, so the promise
    * completes only once the body is in, keeping the request timeout over the whole exchange.
    */
  type StrictResponse = (StatusCode, ByteString)

  /** A connection pool as Pekko hands it out: a flow from a request and its tag to the response, or the transport
    * failure, and the same tag. The tag is the promise the caller waits on, completed with the fully read body.
    */
  type Pool = Flow[(HttpRequest, Promise[StrictResponse]), (Try[HttpResponse], Promise[StrictResponse]), Any]

  /** How a profile gets its pool. Production asks Pekko for a new one; tests pass a flow that answers in-process. */
  type PoolFor = (LlmProfile, ConnectionPoolSettings) => Pool

  /** A pool of this profile's own, never shared, on the host and port of its base url. */
  def pekkoPool(implicit system: ActorSystem): PoolFor = (profile, settings) => {
    val uri = Uri(profile.baseUrl)
    val host = uri.authority.host.address
    if (uri.scheme == "https") Http(system).newHostConnectionPoolHttps(host, uri.effectivePort, settings = settings)
    else Http(system).newHostConnectionPool(host, uri.effectivePort, settings)
  }

  /** One JSON exchange with one profile's endpoint. The only effectful function in the package; production
    * builds a [[PostToLlmProfile]], tests pass a stub.
    */
  type Post = (String, List[HttpHeader], Json) => Future[Json]

  /** Waiting calls allowed per concurrency slot before a call is rejected. */
  val queueDepthPerSlot: Int = 128

  /** Ceiling on a whole-read response body, far above any real answer; Pekko's `max-content-length` is infinite. */
  val maxResponseBytes: Long = 8L * 1024 * 1024
}

/** The [[LlmHttp.Post]] for one profile: holds the profile's connection pool, its waiting queue, and its
  * meters, and runs each call's timeout and retries. Construction starts the pool and registers the meters,
  * which is why one is built per profile at startup and not per call.
  *
  * @param minBackoff wait before the first retry, doubled with jitter up to four seconds; tests shorten it
  * @param maxResponseBytes ceiling on a response body; tests shrink it
  */
final class PostToLlmProfile(
  profileName: String,
  profile: LlmProfile,
  poolFor: LlmHttp.PoolFor,
  metrics: MetricRegistry,
  minBackoff: FiniteDuration = 500.millis,
  maxResponseBytes: Long = LlmHttp.maxResponseBytes,
)(implicit system: ActorSystem, logConfig: LogConfig)
    extends LlmHttp.Post
    with LazySafeLogging {

  import LlmHttp.StrictResponse

  implicit private val ec: ExecutionContext = system.dispatcher
  implicit private val scheduler: Scheduler = system.scheduler

  private val maxErrorBodyExcerpt = 300

  /** Aborts a body read whose promise already settled, so nothing is waiting for the bytes; no caller sees it. */
  private case object AbandonedRead extends Exception("the body read was abandoned") with NoStackTrace

  /** Standard 5xx statuses the HTTP specifications call permanent: the same request fails the same
    * way until someone changes the server or the network. 501 is the one a model server sends,
    * e.g. Ollama 0.40.0 answers 501 to `response_format` for a model without structured output.
    */
  private val permanentServerErrors: Set[Int] = Set(501, 505, 506, 508, 510, 511)

  /** The config restricts profile names to characters legal in an HTTP product token and a metric name
    * component, so the name rides as it is.
    */
  private val userAgent: `User-Agent` = `User-Agent`(s"quine-llm/$profileName")

  // The idle timeout sits above request-timeout so our own timeout fires first, not the pool's 60 second default.
  // Pekko doubles the connection backoff per failed connect, up to a 2 minute default that outlasts any request
  // timeout; capped at one second, a dead endpoint fails fast with its transport error. It governs only failed
  // connects, so it cannot hammer a live server, and it assumes request timeouts well above a second.
  private val queueCapacity: Int = profile.maxConcurrentRequests * LlmHttp.queueDepthPerSlot
  private val poolSettings: ConnectionPoolSettings = ConnectionPoolSettings(system)
    .withMaxConnections(profile.maxConcurrentRequests)
    .withMaxOpenRequests(queueCapacity)
    .withMaxConnectionBackoff(1.second)
    .withConnectionSettings(
      ClientConnectionSettings(system)
        .withIdleTimeout(profile.requestTimeout + 5.seconds)
        .withUserAgentHeader(Some(userAgent)),
    )

  /** Read the response body in full, each failure a classified [[LlmError]] whose message holds no response
    * bytes or host. The request timer is the only deadline: once `settled` completes the read is cancelled,
    * not drained, closing the connection so a stalled or trickling body cannot hold its pool slot.
    */
  private def readBody(response: HttpResponse, settled: Future[StrictResponse]): Future[StrictResponse] = {
    val (stop, folded) = response.entity.dataBytes
      // The ceiling is enforced here rather than through `toStrict(maxBytes)`, whose failure is the same
      // exception class a truncated body raises, so the two could not be classified apart
      .limitWeighted(maxResponseBytes)(_.size.toLong)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.fold(ByteString.empty)(_ ++ _))(Keep.both)
      .run()
    settled.onComplete(_ => stop.abort(AbandonedRead))
    folded
      .map(bytes => (response.status, bytes))
      .recoverWith {
        // The settled promise ignores it, so it is not an error: wrapping it as one would log a
        // misleading line about a read this code cancelled itself
        case AbandonedRead => Future.failed(AbandonedRead)
        case _: StreamLimitReachedException =>
          Future.failed(
            LlmError(
              s"the model endpoint sent a response over $maxResponseBytes bytes",
              LlmError.Kind.Final,
              LlmError.Fault.BadResponse,
              Some(response.status.intValue),
            )(),
          )
        case NonFatal(e) =>
          logger.debug(
            log"Model request from profile ${Safe(profileName)} failed while the response body streamed"
            withException e,
          )
          Future.failed(
            new LlmError(
              s"the connection to the model endpoint failed before the full response arrived (${e.getClass.getSimpleName})",
              LlmError.Kind.Retryable,
              LlmError.Fault.Unavailable,
            )(Some(e)),
          )
      }
  }

  // The queue is the waiting room in front of the pool's max-concurrent-requests slots; a full queue refuses
  // the call. The request timer starts as the pool takes a request, not while it waits, and runs to the last
  // body byte. A timeout is not retried: retrying a 60 second timeout twice would hold a query three minutes.
  private val (queue, done) = Source
    .queue[(HttpRequest, Promise[StrictResponse])](queueCapacity)
    .map { case (request, promise) =>
      // LlmProfile.maxRequestTimeout caps request-timeout, since a delay the scheduler refuses fails this stream
      val expiry = scheduler.scheduleOnce(profile.requestTimeout) {
        promise.tryFailure(
          LlmError(
            s"the model endpoint did not answer within ${profile.requestTimeout}",
            LlmError.Kind.CannotServe,
            LlmError.Fault.Unavailable,
          ),
        )
        ()
      }
      promise.future.onComplete(_ => expiry.cancel())
      (request, promise)
    }
    .via(poolFor(profile, poolSettings))
    .toMat(Sink.foreach { case (sent, promise) =>
      sent match {
        // A response whose promise the timer already settled is cancelled at once by readBody's
        // abort, so a late body never holds its connection
        case Success(response) =>
          promise.completeWith(readBody(response, promise.future))
          ()
        case Failure(e) =>
          promise.tryFailure(e)
          ()
      }
    })(Keep.both)
    .run()
  done.failed.foreach { e =>
    logger.error(log"The connection pool for model profile ${Safe(profileName)} stopped" withException e)
  }

  private def metricName(suffix: String): String =
    MetricRegistry.name(LlmMetricComponent, profileName, suffix)
  private val requests = metrics.meter(metricName("requests"))
  private val attempts = metrics.meter(metricName("attempts"))
  private val failures = metrics.meter(metricName("failures"))
  private val latency = metrics.timer(metricName("latency"))
  private val inputTokens = metrics.meter(metricName("input-tokens"))
  private val outputTokens = metrics.meter(metricName("output-tokens"))

  /** Every api bills by tokens and reports what a call cost in its `usage` object, under its own field names:
    * Jev and Anthropic say `input_tokens` and `output_tokens`, OpenAI says `prompt_tokens` and `completion_tokens`.
    */
  private def markUsage(json: Json): Unit = {
    val usage = json.hcursor.downField("usage")
    def count(names: String*): Option[Long] = names.flatMap(name => usage.get[Long](name).toOption).headOption
    count("input_tokens", "prompt_tokens").foreach(inputTokens.mark)
    count("output_tokens", "completion_tokens").foreach(outputTokens.mark)
  }

  /** Classify the exchange; a timeout or body-read failure is already an [[LlmError]] and passes through. */
  private def readJson(sent: Try[StrictResponse]): Future[Json] = sent match {
    case Failure(e: LlmError) => Future.failed(e)
    case Failure(NonFatal(e)) =>
      // The message names only the class, since the cause may hold the host; the cause rides on the
      // error for the fallback warn, and this debug line is the only record for a single-profile call
      logger.debug(log"Model request from profile ${Safe(profileName)} failed before a response" withException e)
      Future.failed(
        new LlmError(
          s"could not reach the model endpoint (${e.getClass.getSimpleName})",
          LlmError.Kind.Retryable,
          LlmError.Fault.Unavailable,
        )(Some(e)),
      )
    case Failure(fatal) => Future.failed(fatal)
    case Success((statusCode, bytes)) =>
      val text = bytes.utf8String
      val status = statusCode.intValue
      if (!statusCode.isSuccess()) {
        // A 504 is slow by construction, a gateway sends it only after its own timeout, so
        // like our own timeout it is not retried in place and moves to the next profile.
        // Other 5xx and 429 are retried unless the specifications call the status permanent.
        val kind: LlmError.Kind =
          if (status == 504) LlmError.Kind.CannotServe
          else if (status == 429 || (status >= 500 && !permanentServerErrors(status))) LlmError.Kind.Retryable
          else LlmError.Kind.Final
        val fault: LlmError.Fault =
          if (kind == LlmError.Kind.Final) LlmError.Fault.BadResponse else LlmError.Fault.Unavailable
        Future.failed(
          LlmError(
            s"the model endpoint returned status $status: ${text.take(maxErrorBodyExcerpt)}",
            kind,
            fault,
            Some(status),
          )(),
        )
      } else
        parse(text).fold(
          e => Future.failed(LlmError.malformed(s"body is not JSON (${e.getMessage})", Some(e))),
          json => {
            markUsage(json)
            Future.successful(json)
          },
        )
  }

  /** Queue the request. A refusal by the queue is final and never reaches `readJson`. */
  private def send(request: HttpRequest): Future[Json] = {
    val promise = Promise[StrictResponse]()
    queue.offer((request, promise)) match {
      case QueueOfferResult.Enqueued => promise.future.transformWith(readJson)
      case QueueOfferResult.Dropped => Future.failed(LlmError.overloaded(profileName, queueCapacity))
      case _: QueueCompletionResult =>
        Future.failed(
          // The pool is built once at startup and never rebuilt, so this is permanent for the
          // process: an operator must restart it, which is BadResponse, not an outage that clears
          LlmError(
            s"""profile "$profileName" no longer accepts requests, its pool has stopped""",
            LlmError.Kind.CannotServe,
            LlmError.Fault.BadResponse,
          ),
        )
    }
  }

  def apply(uri: String, headers: List[HttpHeader], body: Json): Future[Json] = {
    val payload = body.noSpaces
    // The payload holds untrusted query data, so the redaction settings decide whether it is logged
    logger.debug(log"Sending model request: $payload")
    val request = HttpRequest(HttpMethods.POST, uri, headers, HttpEntity(ContentTypes.`application/json`, payload))
    val attempt: () => Future[Json] = () => {
      attempts.mark()
      send(request)
    }
    requests.mark()
    val timing = latency.time()
    // Pekko's `attempts` counts the retries it may schedule; after them it makes one last call it does not
    // judge, so this is three calls in all: the first, then two retries on a rate limit, server error, or
    // transport failure.
    retry(
      attempt,
      shouldRetry = (_: Json, e: Throwable) =>
        e match {
          case e: LlmError => e.retryable
          case _ => false
        },
      attempts = 2,
      minBackoff = minBackoff,
      maxBackoff = 4.seconds,
      randomFactor = 0.2,
    ).transform { outcome =>
      timing.stop()
      if (outcome.isFailure) failures.mark()
      outcome
    }
  }
}
