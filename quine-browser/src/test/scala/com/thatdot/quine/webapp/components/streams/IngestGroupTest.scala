package com.thatdot.quine.webapp.components.streams

import io.circe.Json
import org.scalatest.LoneElement
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.webapp.v2api.V2ApiTypes.{V2IngestInfo, V2IngestStats, V2RatesSummary}

/** The name-collapse the Streams table relies on: one group per ingest name, members
  * ordered by position, stats summed, status and source type reduced to a shared value
  * or `None` when members disagree.
  */
class IngestGroupTest extends AnyFunSuite with Matchers with LoneElement {

  private def info(
    name: String,
    memberIdx: Option[Int],
    status: String = "Running",
    ingested: Long = 0L,
    oneMinute: Double = 0.0,
    sourceType: String = "NumberIterator",
  ): V2IngestInfo =
    V2IngestInfo(
      name = name,
      status = status,
      message = None,
      sourceType = sourceType,
      sourceId = sourceType,
      stats = V2IngestStats(ingested, V2RatesSummary(ingested, oneMinute), totalRuntime = None),
      memberIdx = memberIdx,
      raw = Json.obj("name" -> Json.fromString(name)),
    )

  test("one group per name, ordered by name, members ordered by position") {
    val groups = IngestGroup.fromInfos(
      List(
        info("b", Some(1)),
        info("a", Some(2)),
        info("a", Some(0)),
      ),
    )
    groups.map(_._1) shouldBe List("a", "b")
    val a = groups.head._2
    a.members.map(_.memberIdx) shouldBe List(Some(0), Some(2))
    a.memberIndices shouldBe List(0, 2)
    a.size shouldBe 2
  }

  test("stats are summed across members") {
    val g = IngestGroup(
      "a",
      List(
        info("a", Some(0), ingested = 100, oneMinute = 10.0),
        info("a", Some(1), ingested = 250, oneMinute = 5.5),
      ),
    )
    g.totalIngestedCount shouldBe 350L
    g.totalRate shouldBe 15.5
  }

  test("agreedStatus is the shared value when members agree") {
    val g = IngestGroup("a", List(info("a", Some(0), "Running"), info("a", Some(1), "Running")))
    g.agreedStatus shouldBe Some("Running")
  }

  test("agreedStatus is None when members disagree") {
    val g = IngestGroup("a", List(info("a", Some(0), "Running"), info("a", Some(1), "Paused")))
    g.agreedStatus shouldBe None
  }

  test("a fully-failed group agrees on Failed") {
    val g = IngestGroup("a", List(info("a", Some(0), "Failed"), info("a", Some(1), "Failed")))
    g.agreedStatus shouldBe Some("Failed")
  }

  test("agreedSourceType is the shared value when members agree") {
    val g = IngestGroup("a", List(info("a", Some(0), sourceType = "Kafka"), info("a", Some(1), sourceType = "Kafka")))
    g.agreedSourceType shouldBe Some("Kafka")
  }

  test("agreedSourceType is None when members disagree") {
    val g = IngestGroup("a", List(info("a", Some(0), sourceType = "Kafka"), info("a", Some(1), sourceType = "File")))
    g.agreedSourceType shouldBe None
  }

  test("single-node ingest with no position collapses to a one-member group with no indices") {
    val groups = IngestGroup.fromInfos(List(info("solo", None)))
    val g = groups.loneElement._2
    g.size shouldBe 1
    g.memberIndices shouldBe empty
  }
}
