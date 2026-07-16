package com.thatdot.quine.webapp.dataservice

import com.raquo.airstream.core.{Observer, Signal}

import com.thatdot.quine.webapp.util.Pot

/** Backpressure capability: the system's flow-control state. The server reports one raw snapshot
  * per reachable cluster member per poll; this slice retains those snapshots and resolves them into
  * a single [[BackpressureView]] for the scope the user has chosen.
  *
  * There is no lookback window: every level is the most recent snapshot's state, and every rate is
  * differenced from the two most recent snapshots. The only thing accumulated across time is exactly
  * those two samples, and only so a rate can be taken — see [[BackpressureStore]].
  *
  * Reducing across cluster members is `max` for levels and `sum` for counters, and the reduction
  * keeps its attribution so a tooltip can name the members at fault (see [[Aggregated]]). Consumers
  * do not see that axis either: [[backpressureSnapshotSignal]] is one fixed shape whatever the
  * selection, so a rendering component never picks a host and never aggregates.
  */
trait BackpressureService {

  /** Entry point for backpressure commands. A separate channel from the sibling slices' dispatches:
    * a component declaring only this slice cannot send namespace or wiretap commands.
    */
  def backpressureDispatch: Observer[BackpressureService.Command]

  /** The resolved view for the currently selected scope (see [[backpressureScopeSignal]]) — already
    * scoped and aggregated. Switches instantly when the scope changes, since every scope resolves the
    * same held history; `Pot` distinguishes loading and fetch failure from an empty system. This is
    * the one signal a diagram binds to follow the user's selection.
    */
  def backpressureSnapshotSignal: Signal[Pot[BackpressureView]]

  /** Every member's headline state, whatever the selected scope — the member picker has to tint the
    * members it is offering to switch *to*, so this cannot come from [[backpressureSnapshotSignal]],
    * which knows only about the members already in scope. Empty on OSS, which has no members.
    */
  def memberStatusSignal: Signal[Map[Int, MemberStatus]]

  /** The selected scope, already validated against the members that actually exist — a selection
    * naming a departed member falls back to [[BackpressureService.Scope.Cluster]] rather than
    * showing an empty view.
    */
  def backpressureScopeSignal: Signal[BackpressureService.Scope]

  /** The cluster members the store has observed, by member, ascending. The option list for a scope
    * picker, and empty on OSS — which has no members, and so should not offer the choice at all.
    */
  def listClusterMembers: Signal[Seq[Int]]

  /** Whether the emitted view is frozen, as set by [[BackpressureService.PauseUpdates]] /
    * [[BackpressureService.ResumeUpdates]]. The freeze lives here, not in the diagram, so it outlives
    * a diagram that unmounts and remounts across an in-app page change: a control that reflects the
    * pause state reads it back from this signal rather than assuming "live" on each mount.
    */
  def backpressurePausedSignal: Signal[Boolean]
}

object BackpressureService {

  /** A state-changing request to the backpressure capability, sent via [[backpressureDispatch]]. */
  sealed trait Command

  /** View the whole cluster, or one member. Takes effect only if the member exists; see
    * [[backpressureScopeSignal]]. `replyTo` receives the scope that actually took effect — the
    * validated fallback — once, so the issuer can reconcile its selection UI when a chosen member
    * has already left the cluster. The switch itself is instant; no fetch is awaited.
    */
  final case class SetScope(scope: Scope, replyTo: Observer[Scope] = Observer.empty) extends Command

  /** Freeze the emitted view. History keeps accumulating underneath, so resuming is instant and the
    * window is intact — unlike freezing the poll, which would leave a hole in the history. `replyTo`
    * is notified once the freeze is in effect.
    */
  final case class PauseUpdates(replyTo: Observer[Unit] = Observer.empty) extends Command

  /** Resume emitting, from the history accumulated while paused. `replyTo` is notified once emitting
    * has resumed.
    */
  final case class ResumeUpdates(replyTo: Observer[Unit] = Observer.empty) extends Command

  /** Which hosts the view covers. */
  sealed trait Scope
  object Scope {

    /** Every reachable member, aggregated — with attribution retained per member. */
    case object Cluster extends Scope

    /** One member. Aggregation still runs, over a one-element host set, so this is the same code
      * path rather than a special case.
      */
    final case class Member(memberIdx: Int) extends Scope

    val default: Scope = Cluster
  }
}
