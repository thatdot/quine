package com.thatdot.quine.app.llm

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.config.{LlmApi, LlmProfile}

/** [[LlmFallback.tryEach]] alone: which failures move to the next client, which surface, and what the
  * error says when no client could serve the call.
  */
class LlmFallbackTest extends AnyFunSuite {

  implicit private val ec: ExecutionContext = ExecutionContext.global
  implicit private val logConfig: LogConfig = LogConfig.permissive

  private val profile = LlmProfile.Completion(LlmApi.OpenAiChat(), "http://chat.test/v1", "m")
  private def client(name: String): CompletionClient =
    CompletionClient(name, profile, _ => Future.failed(new NotImplementedError))

  private def result[A](future: Future[A]): A = Await.result(future, 5.seconds)
  private def failure[A](future: Future[A]): Throwable = Await.result(future.failed, 5.seconds)

  private val timedOut =
    LlmError(
      "the model endpoint did not answer within 10 seconds",
      LlmError.Kind.CannotServe,
      LlmError.Fault.Unavailable,
    )

  test("a client that cannot serve the call is tried once and the next one answers") {
    val first = new AtomicInteger
    val second = new AtomicInteger
    val out = LlmFallback.tryEach("llm.test", List(client("a"), client("b"))) { c =>
      if (c.name == "a") { first.incrementAndGet(); Future.failed(timedOut) }
      else { second.incrementAndGet(); Future.successful("pong") }
    }
    assert(result(out) == "pong")
    assert(first.get() == 1)
    assert(second.get() == 1)
  }

  test("a final error surfaces from any position naming its profile, and later clients never run") {
    val second = new AtomicInteger
    val out = LlmFallback.tryEach("llm.test", List(client("a"), client("b"))) { c =>
      if (c.name == "a") Future.failed(LlmError.refused("no"))
      else { second.incrementAndGet(); Future.successful("pong") }
    }
    assert(
      failure(out) == LlmError(
        "profile \"a\": the model declined the request: no",
        LlmError.Kind.Final,
        LlmError.Fault.BadResponse,
      ),
      "a named error keeps the original kind and fault",
    )
    assert(second.get() == 0)
  }

  test("the fault of a chain's aggregate error: waiting may help, then an operator must act, then the author") {
    val tooLarge = LlmError.promptTooLarge(2000, 1000)
    // The stopped pool is the one CannotServe failure with fault BadResponse, so this is the only
    // way noneCouldServe's BadResponse branch is reached
    val poolStopped = LlmError(
      "profile \"a\" no longer accepts requests, its pool has stopped",
      LlmError.Kind.CannotServe,
      LlmError.Fault.BadResponse,
    )
    val chains = List(
      ("a size limit and an outage, since waiting may help", tooLarge, timedOut, LlmError.Fault.Unavailable),
      ("a size limit and a stopped pool, an operator must act", poolStopped, tooLarge, LlmError.Fault.BadResponse),
      ("a stopped pool and an outage, since waiting may help", poolStopped, timedOut, LlmError.Fault.Unavailable),
      ("every profile over its size limit, the author must shrink it", tooLarge, tooLarge, LlmError.Fault.Query),
    )
    chains.foreach { case (chain, first, second, fault) =>
      val out = LlmFallback.tryEach("llm.test", List(client("a"), client("b"))) { c =>
        Future.failed(if (c.name == "a") first else second)
      }
      val e = failure(out).asInstanceOf[LlmError]
      assert(e.fault == fault, chain)
      assert(e.kind == LlmError.Kind.Final, chain)
    }
  }
}
