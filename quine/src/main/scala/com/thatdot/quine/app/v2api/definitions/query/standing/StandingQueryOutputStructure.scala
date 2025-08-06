package com.thatdot.quine.app.v2api.definitions.query.standing

import sttp.tapir.Schema.annotations.{description, title}

@title("Structure of Output Data")
sealed trait StandingQueryOutputStructure
object StandingQueryOutputStructure {
  import com.thatdot.quine.app.util.StringOps.syntax._

  @title("With Metadata")
  @description(
    """Wraps a Standing Query Result into an object that includes a data field (`data`, which comprises the result data)
      |and a metadata field (`meta`, which comprises information about the result,
      |such as whether the result is a consequence of a positive match).""".asOneLine,
  )
  final case class WithMetadata() extends StandingQueryOutputStructure

  @title("Bare")
  @description(
    """Maintains the structure of a Standing Query Result as provided
      |(i.e. without wrapping the data and adding metadata).""".asOneLine,
  )
  final case class Bare() extends StandingQueryOutputStructure
}
