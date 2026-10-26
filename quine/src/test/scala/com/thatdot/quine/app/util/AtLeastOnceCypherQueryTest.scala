package com.thatdot.quine.app.util

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

import org.apache.pekko.pattern.after
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.Timeout

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, Logger => LogbackLogger}
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.util.AtLeastOnceCypherQuery.ExternalServiceUnavailable
import com.thatdot.quine.compiler.cypher.{CypherHarness, compile, registerUserDefinedProcedure}
import com.thatdot.quine.exceptions.ExternalServiceException
import com.thatdot.quine.graph.cypher.{
  Expr,
  Parameters,
  ProcedureExecutionLocation,
  QueryContext,
  Type,
  UserDefinedProcedure,
  UserDefinedProcedureSignature,
  Value,
}

/** [[AtLeastOnceCypherQuery]]'s retry layers, fed failures by a procedure in the real interpreter, as in production. */
class AtLeastOnceCypherQueryTest extends CypherHarness("at-least-once-cypher-query") {
  import AtLeastOnceCypherQueryTest._

  registerUserDefinedProcedure(ScriptedServiceCall)

  private val compiled =
    compile(
      s"CALL ${ScriptedServiceCall.name}($$that) YIELD response RETURN response",
      unfixedParameters = Seq("that"),
      cache = false,
    )

  private val unavailable = ExternalServiceException(
    "test.service",
    "could not reach the service (ConnectException)",
    ExternalServiceException.Failure.Unavailable,
  )
  private val badResponse = ExternalServiceException(
    "test.service",
    "the service returned status 401: invalid key",
    ExternalServiceException.Failure.BadResponse,
  )

  /** Run the query once for the record naming `script`, with backoff short enough for a test. */
  private def run(
    script: String,
    debugName: String = "test-query",
    minBackoff: FiniteDuration = 20.millis,
    maxBackoff: FiniteDuration = 1.second,
  ): Future[Seq[Vector[Value]]] =
    AtLeastOnceCypherQuery(
      compiled,
      "that",
      debugName,
      unavailableMinBackoff = minBackoff,
      unavailableMaxBackoff = maxBackoff,
    ).stream(Expr.Str(script), cypherHarnessNamespace)(graph)
      .runWith(Sink.seq)

  private def failSlowly(delay: FiniteDuration, e: Throwable): Future[String] =
    after(delay)(Future.failed[String](e))(graph.system)

  /** Captures [[AtLeastOnceCypherQuery]]'s warns while `body` runs; test logging turns `com.thatdot` off. */
  private def capturingWarns[A](body: ListAppender[ILoggingEvent] => Future[A]): Future[A] = {
    val classLogger = LoggerFactory.getLogger(classOf[AtLeastOnceCypherQuery]).asInstanceOf[LogbackLogger]
    val appender = new ListAppender[ILoggingEvent]
    appender.start()
    val (level, additive) = (classLogger.getLevel, classLogger.isAdditive)
    classLogger.setLevel(Level.WARN)
    classLogger.setAdditive(false)
    classLogger.addAppender(appender)
    body(appender).andThen { case _ =>
      classLogger.detachAppender(appender)
      classLogger.setLevel(level)
      classLogger.setAdditive(additive)
    }
  }

  describe("an external service that is unavailable") {

    it("is waited out, and the record yields one full result set once the service answers") {
      val script = ScriptedServiceCall.script("recovers") { attempt =>
        if (attempt <= 2) Future.failed(unavailable) else Future.successful("pong")
      }
      run("recovers").map { rows =>
        assert(rows == Vector(Vector(Expr.Str("pong"))))
        assert(script.calls == 3, "two waits, then the call that answered")
      }
    }

    it("is waited out with a delay that grows, even when each failure takes longer than the first delay to arrive") {
      // Pins withMaxRestarts(Int.MaxValue, 5.minutes). Pekko's default restart window equals minBackoff, and a
      // failure arriving after that window resets the restart count, so without that line every delay stays at
      // minBackoff: each gap stays flat, 100 ms to fail plus a 50 ms wait plus the query's own run time (about 200 ms
      // in all, measured with the line removed), and every other test stays green. With it the waits double, 50, 100,
      // 200, 400, and 800 ms, so the last gap is several times the first.
      val script = ScriptedServiceCall.script("slow") { attempt =>
        if (attempt <= 5) failSlowly(100.millis, unavailable) else Future.successful("pong")
      }
      run("slow", minBackoff = 50.millis, maxBackoff = 5.seconds).map { rows =>
        assert(rows == Vector(Vector(Expr.Str("pong"))))
        val starts = script.startMillis
        val gaps = starts.zip(starts.tail).map { case (earlier, later) => later - earlier }
        assert(gaps.size == 5, s"six calls, five gaps between them: $gaps")
        assert(gaps.last >= 3 * gaps.head, s"the delay between calls must grow, but the gaps were $gaps ms")
      }
    }

    it("is waited out when it arrives wrapped in another exception") {
      val script = ScriptedServiceCall.script("wrapped") { attempt =>
        if (attempt == 1) Future.failed(new RuntimeException("query step failed", unavailable))
        else Future.successful("pong")
      }
      run("wrapped").map { rows =>
        assert(rows == Vector(Vector(Expr.Str("pong"))))
        assert(script.calls == 2)
      }
    }

    it("is recognized through the cause chain, and only when it is the first external service failure there") {
      assert(ExternalServiceUnavailable.unapply(unavailable).contains(unavailable))
      assert(ExternalServiceUnavailable.unapply(new RuntimeException("outer", unavailable)).contains(unavailable))
      assert(ExternalServiceUnavailable.unapply(badResponse).isEmpty)
      assert(
        ExternalServiceUnavailable
          .unapply(
            ExternalServiceException("test.service", "a bad answer", ExternalServiceException.Failure.BadResponse)
              .initCause(unavailable),
          )
          .isEmpty,
        "the first external service failure in the chain decides",
      )
      assert(ExternalServiceUnavailable.unapply(new RuntimeException("unrelated")).isEmpty)
    }
  }

  describe("a bad response from an external service") {

    it("fails the record at once, after one call") {
      val script = ScriptedServiceCall.script("bad")(_ => Future.failed(badResponse))
      recoverToExceptionIf[ExternalServiceException](run("bad")).map { e =>
        assert(e.failure == ExternalServiceException.Failure.BadResponse)
        assert(script.calls == 1, "a bad response is not retried")
      }
    }
  }

  describe("the warn line") {

    it("is logged once per wait, names the stream, and carries the exception; a bad response logs none") {
      ScriptedServiceCall.script("warns") { attempt =>
        if (attempt <= 2) Future.failed(unavailable) else Future.successful("pong")
      }
      ScriptedServiceCall.script("bad-silent")(_ => Future.failed(badResponse))
      capturingWarns { appender =>
        for {
          _ <- run("warns", debugName = "warn-probe-unavailable")
          _ <- run("bad-silent", debugName = "warn-probe-bad").failed
        } yield {
          val events = appender.list.asScala.toVector
          val waits = events.filter(_.getFormattedMessage.contains("warn-probe-unavailable"))
          assert(waits.size == 2, s"one warn per wait: $events")
          assert(waits.forall(_.getLevel == Level.WARN))
          assert(waits.forall(_.getThrowableProxy != null), "the exception rides on the line")
          assert(!events.exists(_.getFormattedMessage.contains("warn-probe-bad")), "a bad response is not waited on")
        }
      }
    }
  }
}

object AtLeastOnceCypherQueryTest {

  /** One record's scripted service: each call runs `answer` with the call's number, starting at one, and records
    * when it began.
    */
  final class Script(answer: Int => Future[String]) {
    private val count = new AtomicInteger(0)
    private val starts = new ConcurrentLinkedQueue[Long]()

    def run(): Future[String] = {
      starts.add(System.nanoTime())
      answer(count.incrementAndGet())
    }

    def calls: Int = count.get

    def startMillis: Vector[Long] = starts.asScala.toVector.map(_ / 1000000L)
  }

  /** Stands in for a procedure that calls an outside service. Its argument names the script that decides how each
    * call goes, so every test drives its own record.
    */
  object ScriptedServiceCall extends UserDefinedProcedure {
    val name = "test.atLeastOnce.scriptedServiceCall"
    val canContainUpdates = false
    val isIdempotent = false
    val canContainAllNodeScan = false

    val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
      arguments = Vector("script" -> Type.Str),
      outputs = Vector("response" -> Type.Str),
      description = "Runs the next step of the named test script.",
    )

    private val scripts = new ConcurrentHashMap[String, Script]()

    def script(key: String)(answer: Int => Future[String]): Script = {
      val script = new Script(answer)
      scripts.put(key, script)
      script
    }

    def call(
      context: QueryContext,
      arguments: Seq[Value],
      location: ProcedureExecutionLocation,
    )(implicit
      parameters: Parameters,
      timeout: Timeout,
      logConfig: LogConfig,
    ): Source[Vector[Value], _] = arguments match {
      case Seq(Expr.Str(key)) => Source.lazyFuture(() => scripts.get(key).run()).map(r => Vector(Expr.Str(r)))
      case other => throw new IllegalArgumentException(s"$name takes one script name, got $other")
    }
  }
}
