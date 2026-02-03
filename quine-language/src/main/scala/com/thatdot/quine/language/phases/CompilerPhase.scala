package com.thatdot.quine.language.phases

import cats.data.OptionT

import com.thatdot.quine.language.phases.CompilerPhase.SimpleCompilerPhaseEffect

trait CompilerPhase[S <: CompilerState, T <: CompilerState, A, B] extends Phase[S, T, A, B] {
  def pure[X](x: X): SimpleCompilerPhaseEffect[S, X] =
    OptionT.pure(x)

  def none[X]: SimpleCompilerPhaseEffect[S, X] =
    OptionT.none
}

object CompilerPhase {
  type SimpleCompilerPhase[S <: CompilerState, A, B] = CompilerPhase[S, S, A, B]
  type SimpleCompilerPhaseEffect[S <: CompilerState, A] = Phase.PhaseEffect[S, S, A]
}
