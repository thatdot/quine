package com.thatdot.quine.language.phases

import com.thatdot.quine.language.diagnostic.Diagnostic

abstract class CompilerState {
  val diagnostics: List[Diagnostic]
}
