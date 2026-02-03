package com.thatdot.quine.language.phases

import cats.data.{IndexedState, OptionT}

import com.thatdot.quine.language.phases.Phase.PhaseEffect

object Phase {
  type Stateful[S, T, A] = IndexedState[S, T, A]
  type PhaseEffect[S, T, A] = OptionT[Stateful[S, T, *], A]
}

trait Phase[S, T, A, B] { self =>
  def process(a: A): PhaseEffect[S, T, B]

  def andThen[U, V, C](
    nextPhase: Phase[U, V, B, C],
  )(implicit ev: Upgrade[T, U], ev2: Upgrade[U, V]): Phase[S, V, A, C] = new Phase[S, V, A, C] {
    override def process(a: A): PhaseEffect[S, V, C] =
      //TODO I'm certain there's a better way to do this. Right now, I believe this
      //     implementation will blow the stack.
      OptionT {
        IndexedState { (initialState: S) =>
          val (nextState, maybeB) = self.process(a).value.run(initialState).value
          maybeB match {
            case Some(value) => nextPhase.process(value).value.run(ev.apply(nextState)).value
            case None => (ev2.apply(ev.apply(nextState)), None)
          }
        }
      }
  }

}
