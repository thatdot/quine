package com.thatdot.quine.app.model.outputs2.query.standing

sealed abstract class StandingQueryPattern extends Product with Serializable
object StandingQueryPattern {

  final case class Cypher(
    query: String,
    mode: StandingQueryMode = StandingQueryMode.DistinctId,
  ) extends StandingQueryPattern

  sealed abstract class StandingQueryMode extends Product with Serializable
  object StandingQueryMode {
    // DomainGraphBranch interpreter
    case object DistinctId extends StandingQueryMode
    // SQv4/Cypher interpreter
    case object MultipleValues extends StandingQueryMode

    case object QuinePattern extends StandingQueryMode

    val values: Seq[StandingQueryMode] = Seq(DistinctId, MultipleValues, QuinePattern)
  }
}
