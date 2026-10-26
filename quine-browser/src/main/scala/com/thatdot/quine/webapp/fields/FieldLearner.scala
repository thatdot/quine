package com.thatdot.quine.webapp.fields

import com.raquo.laminar.api.L._
import io.circe.Json

/** Layer 2 of the field-learning stack: a reactive bridge from a live stream of JSON samples to
  * a growing [[FieldSchema]], with a sample budget after which it freezes.
  *
  * Deliberately generic — it takes an `EventStream[Json]`, not a WebSocket or a tap, so any
  * source works: a wiretap's `matches`, a background-query tap, or a one-shot stream built from
  * hand-pasted JSON. Samples are learned as-is, so this layer stays unaware of any particular
  * frame shape.
  *
  * Nothing happens until [[connect]] is attached to a mounted element — that is what subscribes
  * to the stream (Airstream streams are lazy). When the budget fills, it freezes and fires
  * `onBudgetFilled` exactly once, which the host uses to release the underlying source.
  */
sealed trait SampleStatus
object SampleStatus {

  /** Stream open, samples arriving, budget not yet reached. */
  final case class Collecting(seen: Int, budget: Int) extends SampleStatus

  /** Budget reached — a static snapshot. */
  final case class Frozen(seen: Int) extends SampleStatus

  /** Stream open but nothing has arrived yet. */
  case object Empty extends SampleStatus
}

final class FieldLearner(
  records: EventStream[Json],
  val budget: Int = 20,
  onBudgetFilled: () => Unit = () => (),
) {
  private val schemaVar: Var[FieldSchema] = Var(FieldSchema.empty)
  private val frozenVar: Var[Boolean] = Var(false)

  val schema: Signal[FieldSchema] = schemaVar.signal

  val status: Signal[SampleStatus] =
    Signal.combine(schemaVar.signal, frozenVar.signal).map { case (s, frozen) =>
      if (frozen || (budget > 0 && s.sampleCount >= budget)) SampleStatus.Frozen(s.sampleCount)
      else if (s.sampleCount == 0) SampleStatus.Empty
      else SampleStatus.Collecting(s.sampleCount, budget)
    }

  private def atBudget(s: FieldSchema): Boolean = budget > 0 && s.sampleCount >= budget

  private def freeze(): Unit =
    if (!frozenVar.now()) {
      frozenVar.set(true)
      onBudgetFilled()
    }

  /** Fold one more sample in, unless frozen or at budget. */
  private def absorb(raw: Json): Unit =
    if (!frozenVar.now() && !atBudget(schemaVar.now())) {
      val next = FieldSchema.observe(schemaVar.now(), raw)
      schemaVar.set(next)
      if (atBudget(next)) freeze()
    }

  /** Attach to a mounted element to begin consuming the stream. */
  def connect: Mod[Element] = records --> (raw => absorb(raw))

  /** Feed a single sample directly (e.g. hand-pasted JSON) — same path as streamed samples. */
  def observeOne(json: Json): Unit = absorb(json)

  /** Add a field by hand, holding values of `kind`, for when samples don't show it. Allowed even once
    * the sample budget is full, since it adds no sample.
    */
  def declare(path: FieldPath, kind: JsonKind): Unit = schemaVar.update(FieldSchema.declare(_, path, kind))
}
