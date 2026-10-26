package com.thatdot.quine.app.llm

import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._
import org.scalatest.funsuite.AnyFunSuite

/** The Cypher-facing decision vocabulary. The Jev wire format is covered in [[JevTest]]. */
class DecisionTest extends AnyFunSuite {

  private def json(text: String): Json = parse(text).fold(throw _, identity)

  private def yesNo(instructions: String): Question = Question.YesNo(instructions.asJson)
  private def choice(instructions: String, labels: Vector[String]): Question =
    Question.Choice(instructions.asJson, labels.map(_ -> Json.Null))
  private def score(instructions: String, levels: Vector[String]): Question =
    Question.Score(instructions.asJson, levels)

  test("questions decode from the Cypher-facing shape") {
    val cypherShape = json(
      """{"breach":{"type":"yes-no","instructions":"Breach?"},
        | "language":{"type":"choice","instructions":"Language","options":["en","ru"]},
        | "severity":{"type":"score","instructions":"How bad?","options":["low","mid","high"]}}""".stripMargin,
    )
    val questions = Map[String, Question](
      "breach" -> yesNo("Breach?"),
      "language" -> choice("Language", Vector("en", "ru")),
      "severity" -> score("How bad?", Vector("low", "mid", "high")),
    )
    assert(cypherShape.as[Map[String, Question]] == Right(questions))
    val unknown = json("""{"type":"maybe","instructions":"x"}""").as[Question]
    assert(unknown.left.map(_.message) == Left("type must be one of yes-no, choice, score, got \"maybe\""))
  }

  test("an answer set must answer the questions asked, exactly") {
    val questions = Map[String, Question](
      "breach" -> yesNo("Breach?"),
      "language" -> choice("Language", Vector("en", "ru")),
    )
    val answers = Map[String, Answer](
      "breach" -> Answer.YesNo(0.9),
      "language" -> Answer.Choice("ru", Map("en" -> 0.1, "ru" -> 0.9), Some(0.8)),
    )
    def rejected(reason: String): Either[LlmError, Map[String, Answer]] = Left(LlmError.malformed(reason))

    assert(Decision.checkAnswers(questions, answers) == Right(answers), "a complete, well-typed set passes untouched")
    assert(
      Decision.checkAnswers(questions, answers - "language") == rejected("""no answer for question "language""""),
      "a missing answer must fail loudly: Cypher would read the missing key as null and take the false branch",
    )
    assert(
      Decision.checkAnswers(questions, answers + ("tone" -> Answer.YesNo(0.5))) ==
        rejected("""an answer to "tone", a question that was not asked"""),
    )
    assert(
      Decision.checkAnswers(questions, answers.updated("breach", answers("language"))) ==
        rejected("""the answer to "breach" is a choice answer to a yes-no question"""),
    )
    assert(
      Decision.checkAnswers(questions, answers.updated("language", Answer.Choice("zh", Map.empty, None))) ==
        rejected("""the answer to "language" chose "zh", which is not one of its options"""),
    )
  }
}
