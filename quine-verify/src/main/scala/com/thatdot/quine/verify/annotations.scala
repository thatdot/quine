package com.thatdot.quine.verify

/** Marks a definition as ground truth for the formal model of `protocol`.
  *
  * @param reject non-empty if the member is instead modeled by hand; the text says why
  */
class VerifySource(protocol: String, reject: String = "") extends scala.annotation.StaticAnnotation

/** Marks a definition as modeled by `protocol` only to the extent it is observed. */
class VerifyObservedOnly(protocol: String, why: String) extends scala.annotation.StaticAnnotation

/** Marks a definition as deliberately outside the formal model of `protocol`. */
class VerifyIgnore(protocol: String, why: String) extends scala.annotation.StaticAnnotation
