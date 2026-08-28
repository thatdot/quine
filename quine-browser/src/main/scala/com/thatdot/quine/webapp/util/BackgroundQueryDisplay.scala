package com.thatdot.quine.webapp.util

import scala.scalajs.js

import com.thatdot.quine.webapp.v2api.V2ApiTypes.V2BackgroundQueryStatus

/** How background-query execution records are filtered and ordered for display.
  *
  * Shared by the two surfaces that list them — the query bar's popover and the Streams page's
  * panel — so they can't drift into disagreeing about what "recent" means.
  */
object BackgroundQueryDisplay {

  /** Runs nobody scheduled: started by hand from the query bar rather than dispatched by a job.
    *
    * A job's executions are its own business and belong with it; mixing them in would bury the
    * handful of runs a person actually started under whatever a frequent schedule has produced.
    */
  def adhoc(runs: Seq[V2BackgroundQueryStatus]): Seq[V2BackgroundQueryStatus] =
    runs.filter(_.jobName.isEmpty)

  /** In-flight runs first, then the most recent.
    *
    * Running first because that's the set a person can still act on — watch it, cancel it —
    * while everything else is history.
    */
  def displayOrder(runs: Seq[V2BackgroundQueryStatus]): Seq[V2BackgroundQueryStatus] =
    runs.sortBy(run => (if (run.isRunning) 0 else 1, -expiresAtMillis(run)))

  /** Sort key standing in for "when did this run happen".
    *
    * The API reports no start time, but every record carries `expiresAt`, and expiry is
    * `terminal-or-start time + a per-run retention`. With the default retention shared across
    * runs — overwhelmingly the common case, since neither surface sets one unless asked —
    * ordering by expiry is ordering by recency. A run with a custom retention can sort out of
    * place; that is a cosmetic quirk in a short list, not a correctness problem.
    *
    * An unparseable timestamp sorts last rather than throwing.
    */
  def expiresAtMillis(run: V2BackgroundQueryStatus): Double = {
    val parsed = js.Date.parse(run.expiresAt)
    if (parsed.isNaN) Double.MinValue else parsed
  }
}
