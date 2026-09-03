package com.thatdot.quine.docs

import io.circe.Json

/** One product's capability generator. Objects extending this are the `runMain`
  * entry points: each states what its product ships and nothing else.
  *
  * `main` lives here rather than in each object. Scala emits a static forwarder
  * on a top-level object for every public method it inherits, so `runMain` still
  * finds `public static void main` on the object; the generator tests check that.
  */
trait CapabilityGenerator {

  /** Persistors this product ships, and those it knowingly does not. Together
    * they must name every persistor; see [[Capabilities.persistors]].
    */
  def shipped: Set[String]
  def notShipped: Set[String]

  /** Capability families beyond the common ones. */
  def productFields: Vector[(String, Json)] = Vector.empty

  /** The whole document, in output order. Pure, so tests can assert on it
    * without writing a file.
    */
  final def fields: Vector[(String, Json)] = Capabilities.common(shipped, notShipped) ++ productFields

  final def main(args: Array[String]): Unit =
    Capabilities.write(Capabilities.outputPath(getClass.getSimpleName.stripSuffix("$"), args), fields)
}
