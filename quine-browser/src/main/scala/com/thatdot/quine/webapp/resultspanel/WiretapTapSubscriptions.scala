package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._

/** [[TapSubscriptions]] backed by a per-namespace [[WiretapStore]].
  *
  * The store is held in a `Var` because the host renews it on namespace change: commands
  * resolve against the current store (`now()` on a Var is always safe) and the source view
  * switches with it (via [[WiretapSources.liveSources]], which flat-maps over the option).
  */
final class WiretapTapSubscriptions(storeVar: Var[Option[WiretapStore]]) extends TapSubscriptions {
  def open(key: String, target: TapTarget): Unit =
    storeVar.now().foreach(_.open(key, target))
  def close(key: String): Unit = storeVar.now().foreach(_.close(key))
  // This store models all three tap points and opens the real pre-enrichment socket.
  override val supportsPreEnrichment: Boolean = true
  val sources: Signal[Vector[LiveSource]] = WiretapSources.liveSources(storeVar.signal)
}
