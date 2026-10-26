package com.thatdot.quine.app.llm

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.http.scaladsl.model.HttpHeader
import org.apache.pekko.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}

import cats.syntax.apply._
import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, HCursor, Json}

import com.thatdot.common.security.Secret
import com.thatdot.quine.app.config.LlmProfile
import com.thatdot.quine.app.llm.LlmHttp.Post

/** The Jev `systemone` API, `POST {baseUrl}/v1/systemone`. */
object Jev {

  /** What Jev calls a yes-or-no question. Quine's Cypher surface says `yes-no`. */
  private val yesNoWireType: String = "noul"

  /** Options travel as Jev's `criteria`; instructions pass through, the api takes a string, a map, or a list. */
  private def questionJson(question: Question): Json = {
    def wire(typeName: String, criteria: Option[Json]): Json =
      Json.fromFields(
        List("type" -> typeName.asJson, "instructions" -> question.instructions) ++ criteria.map("criteria" -> _),
      )
    question match {
      case Question.YesNo(_, trueDescription, falseDescription) =>
        val described = List("true" -> trueDescription, "false" -> falseDescription).filterNot(_._2.isNull)
        wire(yesNoWireType, Option.when(described.nonEmpty)(Json.fromFields(described)))
      case choice: Question.Choice =>
        wire(choice.typeName, Some(Json.fromFields(choice.options)))
      case Question.Score(_, options) =>
        wire(question.typeName, Some(options.asJson))
    }
  }

  /** The `systemone` request body: `{model, state, questions: {id: {type, instructions, criteria?}}}`. */
  def requestBody(model: String, state: Json, questions: Map[String, Question]): Json =
    Json.obj(
      "model" -> model.asJson,
      "state" -> state,
      "questions" -> Json.fromFields(questions.map { case (id, q) => id -> questionJson(q) }),
    )

  /** The answer field present gives the question type, not the wire `type`. A score answer keys probabilities by
    * level index with labels in `legend`, so it is re-keyed to the label keys [[Answer]] promises.
    */
  private val answerDecoder: Decoder[Answer] = Decoder.instance { (c: HCursor) =>
    val probabilities = c.get[Option[Map[String, Double]]]("probabilities").map(_.getOrElse(Map.empty))
    val confidence = c.get[Option[Double]]("confidence")
    (c.get[Option[Double]](yesNoWireType), c.get[Option[String]]("choice"), c.get[Option[Double]]("score")) match {
      case (Right(Some(p)), _, _) => Right(Answer.YesNo(p))
      case (_, Right(Some(choice)), _) =>
        for {
          ps <- probabilities
          conf <- confidence
        } yield Answer.Choice(choice, ps, conf)
      case (_, _, Right(Some(score))) =>
        for {
          ps <- probabilities
          legend <- c.get[Option[Map[String, String]]]("legend").map(_.getOrElse(Map.empty))
          conf <- confidence
        } yield Answer.Score(score, ps.map { case (key, p) => legend.getOrElse(key, key) -> p }, conf)
      case _ => Left(DecodingFailure(s"answer has none of $yesNoWireType, choice, score", c.history))
    }
  }

  /** The `systemone` response body: `{answers: {id: answer}}`. */
  def parseResponse(json: Json): Either[DecodingFailure, Map[String, Answer]] = {
    implicit val answers: Decoder[Answer] = answerDecoder
    json.hcursor.get[Map[String, Answer]]("answers")
  }

  /** Build the client for one profile, fixing the URI and headers once. */
  def decide(profile: LlmProfile.Decision, post: Post): Decision.Decide = {
    import Secret.Unsafe._
    val uri: String = profile.baseUrl.stripSuffix("/") + "/v1/systemone"
    val headers: List[HttpHeader] =
      profile.apiKey.map(key => Authorization(OAuth2BearerToken(key.unsafeValue))).toList
    // Both continuations are CPU-only and non-blocking, so neither needs a dispatcher hop: the first
    // renders the payload and offers it to the pool queue, the second walks an already-arrived response
    (state, questions) =>
      Future
        .fromTry((Decision.checkSize(state, profile.maxStateChars) *> Question.validateAll(questions)).toTry)
        .flatMap(_ => post(uri, headers, requestBody(profile.model, state, questions)))(ExecutionContext.parasitic)
        .flatMap { json =>
          Future.fromTry(
            parseResponse(json).left
              .map(f => LlmError.malformed(f.getMessage, Some(f)))
              .flatMap(Decision.checkAnswers(questions, _))
              .toTry,
          )
        }(ExecutionContext.parasitic)
  }
}
