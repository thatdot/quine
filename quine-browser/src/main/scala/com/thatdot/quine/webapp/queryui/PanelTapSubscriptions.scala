package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.resultspanel.{
  LiveSource,
  Provenance,
  SourceKind,
  SourceStatus,
  TapPoint,
  TapSubscriptions,
  TapTarget,
}

/** Backs the results panel's [[TapSubscriptions]] with the shared cross-namespace
  * [[WiretapStore]], scoped to a single `(namespace, owner)` slice.
  *
  * The `(namespace, owner)` pair is the visibility/control boundary: `sources` lists
  * only handlers opened under it, and `open`/`close` act under it. So a panel mounted
  * for one graph won't see another graph's taps, and different UI surfaces on the same
  * graph can use different owners to stay independent (Streams-page taps and Explorer
  * Settings tap queries, for example, don't collide).
  *
  * @param namespace strict namespace signal: drives which slice `sources` observes,
  *                  and its `.now()` gives `open`/`close` the current value synchronously.
  */
final class PanelTapSubscriptions(
  store: WiretapStore,
  namespace: StrictSignal[String],
  owner: WiretapOwner,
) extends TapSubscriptions {

  def open(key: String, target: TapTarget): Unit =
    PanelTapSubscriptions.outputNameOf(target.tapPoint).foreach { outputName =>
      store.open(namespace.now(), owner, key, target.sqName, outputName)
    }

  def close(key: String): Unit =
    store.close(namespace.now(), owner, key)

  val sources: Signal[Vector[LiveSource]] =
    store.active.combineWith(namespace).map { case (active, ns) =>
      active.getOrElse((ns, owner), Nil).toVector.map(PanelTapSubscriptions.adapt)
    }
}

object PanelTapSubscriptions {

  /** Map a [[TapPoint]] to the store's `outputName` (raw = None, post = Some(output)).
    * Pre-enrichment isn't representable in the store yet — and is gated out of the picker —
    * so opening it is a no-op (`None` here means "don't open") until the store models it.
    */
  private def outputNameOf(tapPoint: TapPoint): Option[Option[String]] = tapPoint match {
    case TapPoint.Raw => Some(None)
    case TapPoint.PostEnrichment(out) => Some(Some(out))
    case TapPoint.PreEnrichment(_) => None
  }

  /** Recover the panel's [[TapPoint]] from the store's `outputName`. */
  private def tapPointOf(outputName: Option[String]): TapPoint =
    outputName.fold[TapPoint](TapPoint.Raw)(TapPoint.PostEnrichment(_))

  private def adapt(h: WiretapHandler): LiveSource =
    LiveSource(
      id = h.sourceKey,
      provenance = Provenance(SourceKind.Tap, label(h)),
      status = h.status.signal.map(toSourceStatus),
      records = h.matches,
      tapTarget = Some(TapTarget(h.sqName, tapPointOf(h.outputName))),
    )

  private def label(h: WiretapHandler): String =
    h.outputName.fold(s"${h.sqName} · raw")(out => s"${h.sqName}/$out · post")

  private def toSourceStatus(s: WiretapStatus): SourceStatus = s match {
    case WiretapStatus.Connecting => SourceStatus.Connecting
    case WiretapStatus.Live => SourceStatus.Live
    case WiretapStatus.Error(m) => SourceStatus.Error(m)
    case WiretapStatus.Closed => SourceStatus.Ended
  }
}
