package com.thatdot.quine.webapp.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2BackgroundQueryStatus

/** Filtering and ordering shared by the query bar's popover and the Streams page's panel. */
class BackgroundQueryDisplayTest extends AnyFunSuite with Matchers {

  private def run(
    id: String,
    status: String = V2BackgroundQueryStatus.StatusCompleted,
    jobName: Option[String] = None,
    expiresAt: String = "2026-08-18T00:00:00Z",
  ) = V2BackgroundQueryStatus(
    id = id,
    jobName = jobName,
    name = None,
    query = "MATCH (n) RETURN n",
    status = status,
    hostId = "local",
    totalRowCount = None,
    columns = None,
    error = None,
    expiresAt = expiresAt,
  )

  private def ids(runs: Seq[V2BackgroundQueryStatus]): Seq[String] = runs.map(_.id)

  test("job-dispatched runs are filtered out of the ad-hoc list") {
    val runs = Seq(run("a"), run("b", jobName = Some("nightly")), run("c"))
    ids(BackgroundQueryDisplay.adhoc(runs)) shouldBe Seq("a", "c")
  }

  test("a run whose job name is blank still counts as job-dispatched") {
    // The field's presence is what marks the run as a job's, not its content.
    BackgroundQueryDisplay.adhoc(Seq(run("a", jobName = Some("")))) shouldBe empty
  }

  test("running executions sort above terminal ones") {
    val runs = Seq(
      run("done", expiresAt = "2026-08-20T00:00:00Z"),
      run("running", status = V2BackgroundQueryStatus.StatusStarted, expiresAt = "2026-08-10T00:00:00Z"),
    )
    // Even though `done` is the more recent record, the in-flight run leads: it is the one
    // still worth acting on.
    ids(BackgroundQueryDisplay.displayOrder(runs)) shouldBe Seq("running", "done")
  }

  test("within each group, the most recent comes first") {
    val runs = Seq(
      run("old", expiresAt = "2026-08-10T00:00:00Z"),
      run("new", expiresAt = "2026-08-20T00:00:00Z"),
      run("runningOld", status = V2BackgroundQueryStatus.StatusStarted, expiresAt = "2026-08-11T00:00:00Z"),
      run("runningNew", status = V2BackgroundQueryStatus.StatusStarted, expiresAt = "2026-08-21T00:00:00Z"),
    )
    ids(BackgroundQueryDisplay.displayOrder(runs)) shouldBe Seq("runningNew", "runningOld", "new", "old")
  }

  test("failed and cancelled runs are terminal, so they sort with the rest of the history") {
    val runs = Seq(
      run("failed", status = V2BackgroundQueryStatus.StatusFailed, expiresAt = "2026-08-20T00:00:00Z"),
      run("cancelled", status = V2BackgroundQueryStatus.StatusCancelled, expiresAt = "2026-08-19T00:00:00Z"),
      run("running", status = V2BackgroundQueryStatus.StatusStarted, expiresAt = "2026-08-01T00:00:00Z"),
    )
    ids(BackgroundQueryDisplay.displayOrder(runs)).head shouldBe "running"
  }

  test("an unparseable expiry sorts last instead of throwing") {
    val runs = Seq(run("bogus", expiresAt = "not a timestamp"), run("fine"))
    ids(BackgroundQueryDisplay.displayOrder(runs)) shouldBe Seq("fine", "bogus")
  }

  test("ordering is stable enough to be a total order over equal keys") {
    val runs = (1 to 5).map(i => run(s"r$i"))
    ids(BackgroundQueryDisplay.displayOrder(runs)) shouldBe Seq("r1", "r2", "r3", "r4", "r5")
  }
}
