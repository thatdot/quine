package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._

/** The capability boundary for standing-query tap subscriptions.
  *
  * This is the target architecture's subscription capability specialized to taps: the panel
  * (and, later, the dashboard and streams page) open and close taps through it and read the
  * resulting [[LiveSource]]s, without depending on the concrete tap machinery (the
  * DataService-owned wiretap store). A
  * `LiveSource` is the per-consumer subscription handle (status + records + stop). Writes
  * flow down (the `open`/`close` methods); state flows up (the `sources` signal).
  */
trait TapSubscriptions {

  /** Open (subscribe to) a tap on `target` under `key`; idempotent per key. */
  def open(key: String, target: TapTarget): Unit

  /** Close (unsubscribe) the tap held under `key`; idempotent. */
  def close(key: String): Unit

  /** Whether this backing can open the pre-enrichment tap point. The server and
    * [[TapPoint.PreEnrichment]] model it, but a given store may not yet — the picker offers
    * the Pre point only when this is `true`. Defaults off; capable backings override it.
    */
  def supportsPreEnrichment: Boolean = false

  /** The current set of viewable tap sources. */
  def sources: Signal[Vector[LiveSource]]
}

object TapSubscriptions {

  /** No subscription backend (no store mounted): open/close are no-ops, sources are empty. */
  val empty: TapSubscriptions = new TapSubscriptions {
    def open(key: String, target: TapTarget): Unit = ()
    def close(key: String): Unit = ()
    val sources: Signal[Vector[LiveSource]] = Signal.fromValue(Vector.empty)
  }
}
