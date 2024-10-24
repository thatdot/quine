package com.thatdot.quine.app.ingest2.sources

import cats.data.ValidatedNel

import com.thatdot.quine.app.ingest2.source.FramedSource
import com.thatdot.quine.util.BaseError

abstract class FramedSourceProvider[T] {

  val validators: List[PartialFunction[T, String]] = List()

  /** Attempt to build a framed source. Validation failures
    * are returned as part of the ValidatedNel failures.
    */
  def framedSource: ValidatedNel[BaseError, FramedSource]

}
