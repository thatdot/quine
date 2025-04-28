package com.thatdot.quine.app.v2api.definitions.query.standing

import sttp.tapir.Schema.annotations.{description, title}

@title("Structure of Output Data")
sealed trait StandingQueryOutputStructure
object StandingQueryOutputStructure {

  @title("WithMetadata")
  @description("Output the result wrapped in an object with a field for the metadata and a field for the query result")
  final case class WithMetadata() extends StandingQueryOutputStructure

  @title("Bare")
  @description(
    """Output the result as is with no metadata. Warning: if this is used with `includeCancellations=true` then there
      |will be no way to determine the difference between positive and negative matches""".stripMargin,
  )
  final case class Bare() extends StandingQueryOutputStructure
}
