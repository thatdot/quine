package com.thatdot.quine.app.llm

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.app.config.{LlmApi, LlmProfile}

class JevTest extends AnyFunSuite {

  private def parsed(text: String): Json = parse(text).fold(throw _, identity)

  test("the response decodes the three answer shapes, re-keying a score's legend indices to labels") {
    val response = parsed(
      """{"model":"jev-1.13.0","answers":{
        |   "breach":{"type":"noul","noul":0.87},
        |   "language":{"type":"choice","choice":"ru","probabilities":{"en":0.05,"ru":0.91,"zh":0.01,"other":0.03},"confidence":0.86},
        |   "severity":{"type":"score","score":1.52,"legend":{"0":"low","1":"mid","2":"high"},
        |               "probabilities":{"0":0.1,"1":0.5,"2":0.4},"confidence":0.53}},
        | "usage":{"input_tokens":142,"output_tokens":70}}""".stripMargin,
    )
    assert(
      Jev.parseResponse(response) == Right(
        Map(
          "breach" -> Answer.YesNo(0.87),
          "language" -> Answer
            .Choice("ru", Map("en" -> 0.05, "ru" -> 0.91, "zh" -> 0.01, "other" -> 0.03), Some(0.86)),
          "severity" -> Answer.Score(1.52, Map("low" -> 0.1, "mid" -> 0.5, "high" -> 0.4), Some(0.53)),
        ),
      ),
    )
    assert(
      Jev.parseResponse(
        parsed("""{"answers":{"s":{"score":2.4,"probabilities":{"low":0.1,"high":0.9},"confidence":0.5}}}"""),
      ) == Right(Map("s" -> Answer.Score(2.4, Map("low" -> 0.1, "high" -> 0.9), Some(0.5)))),
      "a label-keyed score answer without a legend passes through as it is",
    )
    assert(
      Jev.parseResponse(parsed("""{"answers":{"x":{"legend":"?"}}}""")).isLeft,
      "an answer must have one of noul, choice, score",
    )
  }

  test("a state over max-state-chars fails before any request leaves, and an unset limit sends everything") {
    val profile = LlmProfile.Decision(api = LlmApi.JevSystemOne, baseUrl = "http://model.test", model = "m")
    val stub = LlmStubPost.answering(parsed("""{"answers":{"breach":{"noul":0.9}}}"""))
    val questions = Map[String, Question]("breach" -> Question.YesNo("Breach?".asJson))
    // The serialized state is the 100 characters plus the two quotes: 102
    val state = ("x" * 100).asJson
    val limited = Jev.decide(profile.copy(maxStateChars = Some(40)), stub.post)
    val error = intercept[LlmError](Await.result(limited(state, questions), 10.seconds))
    assert(error.getMessage == "state is 102 characters, over the profile limit of 40 (max-state-chars)")
    assert(error.cannotServe, "the limit is the profile's own, so a fallback profile may serve the call")
    assert(stub.received.isEmpty, "the request never left")
    assert(
      Await.result(Jev.decide(profile, stub.post)(state, questions), 10.seconds) == Map("breach" -> Answer.YesNo(0.9)),
      "no limit on the same state",
    )
  }

  test("decide rejects a response that skips a question, before Cypher reads the gap as null") {
    val profile = LlmProfile.Decision(api = LlmApi.JevSystemOne, baseUrl = "http://model.test", model = "m")
    val stub = LlmStubPost.answering(parsed("""{"answers":{"breach":{"noul":0.9}}}"""))
    val questions = Map[String, Question](
      "breach" -> Question.YesNo("Breach?".asJson),
      "language" -> Question.Choice("Language".asJson, Vector("en" -> Json.Null, "ru" -> Json.Null)),
    )
    val decide: Decision.Decide = Jev.decide(profile, stub.post)
    val error = intercept[LlmError](Await.result(decide("x".asJson, questions), 10.seconds))
    assert(
      error.getMessage == """the model endpoint returned an unexpected response: no answer for question "language"""",
    )
    assert(!error.cannotServe, "the endpoint answered, badly, so no fallback profile changes the outcome")
    assert(error.fault == LlmError.Fault.BadResponse)
  }
}
