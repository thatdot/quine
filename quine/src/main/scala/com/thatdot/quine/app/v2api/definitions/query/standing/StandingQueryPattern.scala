package com.thatdot.quine.app.v2api.definitions.query.standing

import sttp.tapir.Schema.annotations.{default, description, title}

@title("Standing Query Pattern")
@description("A declarative structural graph pattern.")
sealed abstract class StandingQueryPattern
object StandingQueryPattern {
  import com.thatdot.quine.app.util.StringOps.syntax._

  @title("Cypher")
  final case class Cypher(
    @description(
      """Cypher query describing the Standing Query pattern. This must take the form of
        |`MATCH <pattern> WHERE <condition> RETURN <columns>`. When the `mode` is `DistinctId`,
        |the `RETURN` must also be `DISTINCT`.""".asOneLine,
    )
    query: String,
    @default(StandingQueryMode.DistinctId)
    mode: StandingQueryMode = StandingQueryMode.DistinctId,
  ) extends StandingQueryPattern

  sealed abstract class StandingQueryMode
  object StandingQueryMode {
    // DomainGraphBranch interpreter
    case object DistinctId extends StandingQueryMode
    // SQv4/Cypher interpreter
    case object MultipleValues extends StandingQueryMode

    case object QuinePattern extends StandingQueryMode

    val values: Seq[StandingQueryMode] = Seq(DistinctId, MultipleValues, QuinePattern)
  }
}
