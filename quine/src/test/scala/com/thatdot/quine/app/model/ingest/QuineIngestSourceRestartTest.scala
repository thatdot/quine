package com.thatdot.quine.app.model.ingest

import java.util.concurrent.TimeoutException

import org.apache.pekko.pattern.AskTimeoutException
import org.apache.pekko.stream.RemoteStreamRefActorTerminatedException

import org.apache.kafka.common.KafkaException
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.core.exception.SdkClientException

import com.thatdot.quine.graph.ShardNotAvailableException
import com.thatdot.quine.util.AnyError.GenericError
import com.thatdot.quine.util.ExternalError.RemoteStreamRefActorTerminatedError

/** Classification tests for [[QuineIngestSource.isRestartableIngestFailure]] — the predicate
  * behind an ingest's `restartSettings.withRestartOn`.
  *
  * The remote-relay cases mirror failures observed on a production cluster: an exception that
  * crosses a member boundary arrives as [[GenericError]] (class reduced to a name, original
  * stack preserved) or as an [[com.thatdot.quine.util.ExternalError]] wrapper, never as its
  * original class.
  */
class QuineIngestSourceRestartTest extends AnyFunSuite {

  private def isRestartable(e: Throwable): Boolean = QuineIngestSource.isRestartableIngestFailure(e)

  private val streamRefFrame = new StackTraceElement(
    "org.apache.pekko.stream.impl.streamref.SourceRefStageImpl$$anon$1",
    "$anonfun$receiveRemoteMessage$1",
    "SourceRefImpl.scala",
    232,
  )
  private val unrelatedFrame = new StackTraceElement("com.example.UserCode", "run", "UserCode.scala", 10)

  private def withStack(e: Throwable, frame: StackTraceElement): Throwable = {
    e.setStackTrace(Array(frame))
    e
  }

  test("local transient failures restart") {
    assert(isRestartable(new KafkaException("broker went away")))
    assert(isRestartable(SdkClientException.create("throttled")))
    assert(isRestartable(new TimeoutException("commit timeout")))
    // pekko-kafka's CommitTimeoutException and AskTimeoutException are j.u.c.TimeoutExceptions
    assert(isRestartable(new AskTimeoutException("Ask timed out on [...] after [32000 ms]")))
    assert(isRestartable(new org.apache.pekko.kafka.CommitTimeoutException("Kafka commit took longer than 15s")))
    assert(isRestartable(RemoteStreamRefActorTerminatedException("remote terminated")))
    assert(isRestartable(ShardNotAvailableException("shard 12 dropped out")))
  }

  test("a bare IllegalStateException thrown by pekko's stream-ref plumbing restarts") {
    val e = withStack(
      new IllegalStateException("[SourceRef-1] Got unexpected OnSubscribeHandshake(...) in state UpstreamTerminated"),
      streamRefFrame,
    )
    assert(isRestartable(e))
  }

  test("a bare IllegalStateException from anywhere else does not restart") {
    assert(!isRestartable(withStack(new IllegalStateException("user code bug"), unrelatedFrame)))
  }

  test("a stream-ref IllegalStateException relayed from another member as GenericError restarts") {
    // The shape observed in production: exceptionType is the original FQCN, message and stack preserved.
    val relayed = GenericError(
      "java.lang.IllegalStateException",
      "[SourceRef-176107750] Got unexpected OnSubscribeHandshake(...) in state UpstreamTerminated(...)",
      Array(streamRefFrame),
      None,
    )
    assert(isRestartable(relayed))
  }

  test("a whitelisted exception relayed as GenericError restarts, honoring subtyping by name") {
    val relayed = GenericError(
      "org.apache.pekko.pattern.AskTimeoutException",
      "Ask timed out on [...] after [32000 ms]",
      Array(unrelatedFrame),
      None,
    )
    assert(isRestartable(relayed))
  }

  test("a non-transient failure relayed as GenericError does not restart") {
    val relayed = GenericError("java.lang.NullPointerException", "boom", Array(unrelatedFrame), None)
    assert(!isRestartable(relayed))
  }

  test("a GenericError whose cause chain contains a transient failure restarts") {
    val cause = GenericError(
      "org.apache.kafka.common.KafkaException",
      "broker went away",
      Array(unrelatedFrame),
      None,
    )
    val relayed = GenericError("java.lang.RuntimeException", "wrapper", Array(unrelatedFrame), Some(cause))
    assert(isRestartable(relayed))
  }

  test("ExternalError wrappers restart when their wrapped exception would") {
    val wrapped = RemoteStreamRefActorTerminatedError(RemoteStreamRefActorTerminatedException("remote terminated"))
    assert(isRestartable(wrapped))
  }

  test("local non-transient failures do not restart") {
    assert(!isRestartable(new RuntimeException("boom")))
    assert(!isRestartable(withStack(new NullPointerException("npe"), unrelatedFrame)))
  }

  test("a local failure whose cause is transient restarts") {
    assert(isRestartable(new RuntimeException("wrapper", new TimeoutException("underlying timeout"))))
  }
}

/** `QuineIngestSource.restartSettings` reaches recipe-decoded values without tapir's API-layer
  * validators (recipes decode via plain circe), so it must clamp out-of-range restart-policy
  * inputs itself. Pekko's restart-budget check is exact equality (`restartCount == maxRestarts`),
  * so a negative `maxRestarts` never trips it, and a non-positive `within` makes the reset
  * deadline always overdue — both would otherwise cause infinite restarts.
  */
class QuineIngestSourceRestartSettingsClampTest extends AnyFunSuite {
  import scala.concurrent.duration._

  test("negative maxRestarts is clamped to 0") {
    val settings = QuineIngestSource.restartSettings(-1, 31.seconds, 10.seconds, 10.seconds)
    assert(settings.maxRestarts == 0)
  }

  test("non-positive within is clamped to a positive duration") {
    val settings = QuineIngestSource.restartSettings(3, Duration.Zero, 10.seconds, 10.seconds)
    assert(settings.maxRestartsWithin > Duration.Zero)
  }

  test("non-positive minBackoff and maxBackoff are clamped to positive durations") {
    val settings = QuineIngestSource.restartSettings(3, 31.seconds, Duration.Zero, Duration.Zero)
    assert(settings.minBackoff > Duration.Zero)
    assert(settings.maxBackoff > Duration.Zero)
  }

  test("valid inputs pass through unchanged") {
    val settings = QuineIngestSource.restartSettings(3, 31.seconds, 10.seconds, 10.seconds)
    assert(settings.maxRestarts == 3)
    assert(settings.maxRestartsWithin == 31.seconds)
    assert(settings.minBackoff == 10.seconds)
    assert(settings.maxBackoff == 10.seconds)
  }
}
