package com.thatdot.quine.compiler.cypher

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.pattern.Patterns
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Sink}
import org.apache.pekko.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AsyncFunSpec

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.common.logging.Pretty.PrettyHelper
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph._
import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.persistor._

/** The `history.*` procedures answer from the journal, so with journal persistence switched off they
  * have nothing to read. They must say so rather than returning an empty result that looks
  * indistinguishable from a node which genuinely never changed.
  */
class HistoricalJournalDisabledTests extends AsyncFunSpec with BeforeAndAfterAll {
  // Declared before `graph`, which reads them while being initialized
  val timeout: Timeout = Timeout(10.seconds)
  implicit val relayAskTimeout: Timeout = Timeout(3.seconds)
  implicit val idProv: QuineIdLongProvider = QuineIdLongProvider()
  implicit protected val logConfig: LogConfig = LogConfig.permissive

  private def journalDisabledPersistor: ActorSystem => PrimePersistor = { _ =>
    new StatelessPrimePersistor(
      PersistenceConfig(journalEnabled = false),
      None,
      (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns)(LogConfig.strict),
      slug = "in-memory-no-journal",
    )(null, LogConfig.strict)
  }

  lazy val graph: BaseGraph with CypherOpsGraph with LiteralOpsGraph = Await.result(
    GraphService(
      "historical-journal-disabled-tests",
      effectOrder = EventEffectOrder.PersistorFirst,
      persistorMaker = journalDisabledPersistor,
      idProvider = idProv,
    ),
    timeout.duration,
  )

  implicit val ec: ExecutionContextExecutor = graph.system.dispatcher
  val namespace: NamespaceId = defaultNamespaceId
  implicit def materializer: Materializer = graph.materializer
  private val nodeA: QuineId = idProv.customIdToQid(500L)
  private val nodeB: QuineId = idProv.customIdToQid(501L)

  override def beforeAll(): Unit =
    Await.result(
      Patterns.retry(
        () => Future(graph.requiredGraphIsReady()),
        attempts = 100,
        delay = 200.millis,
        graph.system.scheduler,
        graph.system.dispatcher,
      ),
      timeout.duration,
    )

  override def afterAll(): Unit =
    Await.result(graph.shutdown(), timeout.duration * 2L)

  private def expectJournalDisabledError(queryText: String) =
    recoverToExceptionIf[CypherException.Runtime] {
      queryCypherValues(queryText, namespace)(graph).results.toMat(Sink.seq)(Keep.right).run()
    }.map { err =>
      assert(
        err.getMessage.contains("requires the journal to be enabled"),
        s"Expected a journal-disabled error, got: ${err.getMessage}",
      )
    }

  it("history.propertyChanges reports that the journal is disabled") {
    expectJournalDisabledError(
      s"""MATCH (n) WHERE strId(n) = "${nodeA.pretty}"
         |CALL history.propertyChanges(n, "anything") YIELD key, value, changeTime
         |RETURN key, value, changeTime""".stripMargin,
    )
  }

  it("history.edgeChanges reports that the journal is disabled") {
    expectJournalDisabledError(
      s"""MATCH (source) WHERE strId(source) = "${nodeA.pretty}"
         |MATCH (target) WHERE strId(target) = "${nodeB.pretty}"
         |CALL history.edgeChanges(source, target, "ANYTHING", null) YIELD action, edgeType, changeTime
         |RETURN action, edgeType, changeTime""".stripMargin,
    )
  }

  it("history.nodeAt reports that the journal is disabled") {
    // A node is rebuilt at a past moment by replaying its journal from the most recent snapshot,
    // so without the journal there is nothing to rebuild it from.
    expectJournalDisabledError(
      s"""MATCH (n) WHERE strId(n) = "${nodeA.pretty}"
         |CALL history.nodeAt(n, 1) YIELD node, edges
         |RETURN node, edges""".stripMargin,
    )
  }

  it("history.edgeChangesBetween reports that the journal is disabled") {
    expectJournalDisabledError(
      s"""MATCH (source) WHERE strId(source) = "${nodeA.pretty}"
         |MATCH (target) WHERE strId(target) = "${nodeB.pretty}"
         |CALL history.edgeChangesBetween(source, target, "ANYTHING", null) YIELD action, edgeType, changeTime
         |RETURN action, edgeType, changeTime""".stripMargin,
    )
  }

  it("history.nodeChanges reports that the journal is disabled") {
    expectJournalDisabledError(
      s"""MATCH (n) WHERE strId(n) = "${nodeA.pretty}"
         |CALL history.nodeChanges(n) YIELD kind, detail, changeTime
         |RETURN kind, detail, changeTime""".stripMargin,
    )
  }

  it("history.queryAt reports that the journal is disabled") {
    // Running a query at a past moment rebuilds each node it touches by replaying that node's
    // journal, so without one the query would quietly answer from whatever the present holds.
    expectJournalDisabledError(
      s"""CALL history.queryAt('MATCH (m) WHERE strId(m) = "${nodeA.pretty}" RETURN m', 1)
         |YIELD value RETURN value""".stripMargin,
    )
  }
}
