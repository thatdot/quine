package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.dataservice.{
  WiretapHandler,
  WiretapOwner,
  WiretapService,
  WiretapStatus,
  WiretapTapPoint,
}
import com.thatdot.quine.webapp.resultspanel.{
  LiveSource,
  Provenance,
  SourceKind,
  SourceStatus,
  TapPoint,
  TapSubscriptions,
  TapTarget,
}

/** Backs the results panel's [[TapSubscriptions]] with the shared [[WiretapService]],
  * scoped to a single [[WiretapOwner]].
  *
  * The owner is the visibility/control boundary: `sources` lists only the handlers opened
  * under `owner`, and `open`/`close` act under it. So a panel mounted with its own owner
  * sees only the taps started from it - not Streams-page taps or Explorer Settings tap
  * queries, which live under other owners (and still share sockets by source). To make
  * several surfaces share one tap set, mount them with the same owner.
  */
final class PanelTapSubscriptions(wiretap: WiretapService, owner: WiretapOwner) extends TapSubscriptions {

  def open(key: String, target: TapTarget): Unit =
    wiretap.wiretapDispatch.onNext(
      WiretapService.OpenTap(owner, key, target.sqName, PanelTapSubscriptions.toWiretapTapPoint(target.tapPoint)),
    )

  def close(key: String): Unit = wiretap.wiretapDispatch.onNext(WiretapService.CloseTap(owner, key))

  // The dataservice's WiretapStore models all three tap points (raw, pre-enrichment,
  // post-enrichment), so the picker can safely offer Pre.
  override val supportsPreEnrichment: Boolean = true

  val sources: Signal[Vector[LiveSource]] =
    wiretap.wiretapsSignal.map(_.getOrElse(owner, Nil).toVector.map(PanelTapSubscriptions.adapt))
}

object PanelTapSubscriptions {

  /** Map the panel's [[TapPoint]] to the dataservice's [[WiretapTapPoint]] vocabulary. */
  private def toWiretapTapPoint(tapPoint: TapPoint): WiretapTapPoint = tapPoint match {
    case TapPoint.Raw => WiretapTapPoint.Raw
    case TapPoint.PreEnrichment(out) => WiretapTapPoint.PreEnrichment(out)
    case TapPoint.PostEnrichment(out) => WiretapTapPoint.PostEnrichment(out)
  }

  /** Recover the panel's [[TapPoint]] from the dataservice's [[WiretapTapPoint]]. */
  private def toTapPoint(tapPoint: WiretapTapPoint): TapPoint = tapPoint match {
    case WiretapTapPoint.Raw => TapPoint.Raw
    case WiretapTapPoint.PreEnrichment(out) => TapPoint.PreEnrichment(out)
    case WiretapTapPoint.PostEnrichment(out) => TapPoint.PostEnrichment(out)
  }

  private def adapt(h: WiretapHandler): LiveSource =
    LiveSource(
      id = h.sourceKey,
      provenance = Provenance(SourceKind.Tap, label(h)),
      status = h.status.signal.map(toSourceStatus),
      records = h.matches,
      tapTarget = Some(TapTarget(h.sqName, toTapPoint(h.tapPoint))),
    )

  private def label(h: WiretapHandler): String = h.tapPoint match {
    case WiretapTapPoint.Raw => s"${h.sqName} · raw"
    case WiretapTapPoint.PreEnrichment(out) => s"${h.sqName}/$out · pre"
    case WiretapTapPoint.PostEnrichment(out) => s"${h.sqName}/$out · post"
  }

  private def toSourceStatus(s: WiretapStatus): SourceStatus = s match {
    case WiretapStatus.Connecting => SourceStatus.Connecting
    case WiretapStatus.Live => SourceStatus.Live
    case WiretapStatus.Error(m) => SourceStatus.Error(m)
    case WiretapStatus.Closed => SourceStatus.Ended
  }
}
