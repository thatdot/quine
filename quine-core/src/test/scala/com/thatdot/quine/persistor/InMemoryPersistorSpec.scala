package com.thatdot.quine.persistor

import scala.compat.ExecutionContexts

import com.thatdot.quine.graph.EventTime

class InMemoryPersistorSpec extends PersistenceAgentSpec {

  val persistor: PersistenceAgent = InMemoryPersistor.empty

  // TODO: Move this back into PersitenceAgentSpec once the rest of the peristors implement this.
  describe("deleteJournal") {
    val allQids = Seq(qid0, qid1, qid2, qid3, qid4)
    it("can delete all record events for a given Quine Id") {
      //Future.traverse(allQids)(qid =>
      forAll(allQids)(qid =>
        for {
          _ <- persistor.deleteNodeChangeEvents(qid)
          journalEntries <- persistor.getNodeChangeEventsWithTime(qid, EventTime.MinValue, EventTime.MaxValue)
        } yield journalEntries shouldBe empty
      ).map(_ => succeed)(ExecutionContexts.parasitic)
    }
  }
}
