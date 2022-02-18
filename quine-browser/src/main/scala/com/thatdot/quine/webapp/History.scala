package com.thatdot.quine.webapp

/** Abstract representation of a history timeline, recording arbitrary events
  * and offering the ability to step forward and back through time. Note that
  * in order to use this, you'll need an instance of [[Event]] for your event
  * type.
  *
  * TODO: consider moving checkpointing into this
  *
  * @param past past events, in reverse chronological order
  * @param future future events, in chronological order
  */
final case class History[E](
  past: List[E],
  future: List[E]
) {
  import History.Event

  /** Add a new current event to the history */
  def observe(event: E)(implicit runner: Event[E]): History[E] = {
    runner.applyEvent(event)
    History(event :: past, Nil)
  }

  /** Is there a non-empty past to rewind? */
  def canStepBackward: Boolean = past.nonEmpty

  /** Try to rewind one step */
  def stepBack()(implicit runner: Event[E]): Option[History[E]] = past match {
    case Nil => None
    case event :: newPast =>
      runner.applyEvent(runner.invert(event))
      Some(History(newPast, event :: future))
  }

  /** Is there a non-empty future to advance? */
  def canStepForward: Boolean = future.nonEmpty

  /** Try to step forward one step */
  def stepForward()(implicit runner: Event[E]): Option[History[E]] = future match {
    case Nil => None
    case event :: newFuture =>
      runner.applyEvent(event)
      Some(History(event :: past, newFuture))
  }
}
object History {
  def empty[E]: History[E] = History[E](Nil, Nil)

  /** Typeclass for things that can be events */
  trait Event[E] {

    /** How to "run" an event */
    def applyEvent(event: E): Unit

    /** Produce an event with the inverse effect */
    def invert(event: E): E
  }
}
