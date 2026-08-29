package com.thatdot.quine.graph

import com.thatdot.quine.verify.VerifyObservedOnly

/** A namespace identifier. Construct via [[NamespaceId.apply]], which throws on invalid names. */
@VerifyObservedOnly(
  "ingest_channels",
  "Identity only: the channel protocol scopes a slice by namespace and never reads the name",
)
final case class NamespaceId private (name: String) extends AnyVal

object NamespaceId {
  private val ValidPattern = "[a-z][a-z0-9]{0,15}".r

  @throws[IllegalArgumentException](
    "if the name is not 1-16 lowercase alphanumeric characters starting with a letter",
  )
  def apply(name: String): NamespaceId = {
    require(
      ValidPattern.matches(name),
      s"Invalid namespace name: '$name'. Must be 1-16 lowercase alphanumeric characters starting with a letter.",
    )
    new NamespaceId(name)
  }

  val quine: NamespaceId = new NamespaceId("quine")
}
