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

/** Marks a real-valued definition as modeled by `protocol` on a fixed-point INTEGER scale.
  *
  * Quint has no reals. A quantity that is only ever added, subtracted and compared keeps every one
  * of those relations under a fixed scale, so the model can carry it exactly: the abstraction is in
  * the granularity, not in the semantics. Declaring the scale here rather than picking one
  * inside a spec is the point: it makes the choice reviewable next to the code it abstracts, and a
  * model that assumes a different scale disagrees with a written-down number instead of with a
  * guess nobody recorded.
  *
  * @param scale units per 1.0, e.g. 1000 for thousandths
  */
class VerifyScaled(protocol: String, scale: Int, why: String) extends scala.annotation.StaticAnnotation
