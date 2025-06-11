package com.thatdot.quine.app.model.transformation.polyglot
import org.graalvm.polyglot

import com.thatdot.quine.util.BaseError

trait Transformation {
  def apply(input: Polyglot.HostValue): Either[BaseError, polyglot.Value]
}
