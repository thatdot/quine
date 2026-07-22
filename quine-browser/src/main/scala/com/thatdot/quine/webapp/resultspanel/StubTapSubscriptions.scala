package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.resultspanel.streaming.StubLiveSource

/** A dev-only [[TapSubscriptions]] whose `open` spins up a synthetic [[StubLiveSource]], so the
  * tap picker can be exercised end to end — pick a tap point → a live stub tap appears — with no
  * server and no host wiring. Paired with [[StubTapSubscriptions.catalog]] (a sample standing-query
  * list) and enabled via `?stub-taps`.
  */
final class StubTapSubscriptions extends TapSubscriptions {
  private val opened: Var[Map[String, LiveSource]] = Var(Map.empty)

  def open(key: String, target: TapTarget): Unit =
    if (!opened.now().contains(key))
      opened.update(_ + (key -> StubLiveSource(target.key, target.label, Some(target))))

  // As the producer, the stub stops its own source's emission when its subscription closes.
  def close(key: String): Unit = {
    opened.now().get(key).foreach(_.stop())
    opened.update(_ - key)
  }

  val sources: Signal[Vector[LiveSource]] =
    opened.signal.map(_.values.toVector.distinctBy(_.id).sortBy(_.id))
}

object StubTapSubscriptions {

  /** A sample standing-query catalog for the picker (varied output counts, one no-output SQ,
    * one long name, a mix of transformed, enriched, and bare outputs, and enough entries to
    * exercise the filter).
    */
  private def out(name: String, enriched: Boolean = false, transformed: Boolean = false): TapOutput =
    TapOutput(name, enriched, transformed)
  val catalog: Vector[TapCatalogEntry] = Vector(
    TapCatalogEntry("fraudRing", List(out("slack", enriched = true, transformed = true), out("s3"))),
    TapCatalogEntry("highValueTxn", List(out("webhook", enriched = true))),
    TapCatalogEntry("newAccountVelocity", Nil),
    TapCatalogEntry("cardTestingBurst", List(out("kafka"), out("s3", enriched = true))),
    TapCatalogEntry("velocityCheck", List(out("slack"))),
    TapCatalogEntry("geoAnomaly", List(out("webhook", enriched = true), out("kafka"))),
    TapCatalogEntry("deviceFingerprintCollisionMonitor", List(out("partner-webhook"))),
    TapCatalogEntry("chargebackRing", List(out("s3", enriched = true))),
  )
}
