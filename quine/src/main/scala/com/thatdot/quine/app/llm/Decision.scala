package com.thatdot.quine.app.llm

import scala.concurrent.Future

import cats.syntax.traverse._
import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, Encoder, HCursor, Json}

/** A typed question in the shape `llm.decide` accepts. Cypher map keys arrive sorted (`Expr.Map` is a `SortedMap`),
  * so order that carries meaning belongs in a list, which is preserved.
  */
sealed trait Question extends Product with Serializable {
  def instructions: Json
  def typeName: String
}

object Question {

  /** Pick one labeled option; a label's description, or null, says what counts as it, the model's accuracy lever. */
  final case class Choice(instructions: Json, options: Vector[(String, Json)]) extends Question {
    def typeName: String = "choice"
    def labels: Vector[String] = options.map(_._1)
  }

  /** Place the state on the ordered scale `options`; the answer is an expected level index. */
  final case class Score(instructions: Json, options: Vector[String]) extends Question {
    def typeName: String = "score"
  }

  /** A yes-or-no question, `type: "yes-no"` in Cypher; the answer is a truth probability. The optional
    * descriptions say what counts as true and what counts as false.
    */
  final case class YesNo(
    instructions: Json,
    trueDescription: Json = Json.Null,
    falseDescription: Json = Json.Null,
  ) extends Question {
    def typeName: String = "yes-no"
  }

  val typeNames: String = "yes-no, choice, score"

  val minOptions: Int = 2
  val maxChoiceOptions: Int = 255
  val maxScoreLevels: Int = 10

  /** The shapes instructions and descriptions may take, the same shapes a state may take. */
  private def isStructuredText(json: Json): Boolean = json.isString || json.isObject || json.isArray
  private def isEmptyText(json: Json): Boolean =
    json.asString.exists(_.trim.isEmpty) || json.asObject.exists(_.isEmpty) || json.asArray.exists(_.isEmpty)

  /** Reject questions no decision model would accept, before any request leaves Quine. A blank description is
    * forwarded as given, only a description of the wrong type is rejected.
    */
  def validate(id: String, question: Question): Either[LlmError, Question] = {
    def invalid(reason: String) = Left(LlmError.invalidQuestion(id, reason))
    def checkInstructions: Either[LlmError, Unit] =
      if (!isStructuredText(question.instructions)) invalid("instructions must be a string, a map, or a list")
      else if (isEmptyText(question.instructions)) invalid("instructions must not be empty")
      else Right(())
    def checkDescription(label: String, description: Json): Either[LlmError, Unit] =
      if (description.isNull || isStructuredText(description)) Right(())
      else invalid(s"""the description of "$label" must be a string, a map, a list, or null""")
    def checkLabels(labels: Vector[String], max: Int): Either[LlmError, Unit] =
      if (labels.size < minOptions || labels.size > max)
        invalid(s"needs between $minOptions and $max options, got ${labels.size}")
      else if (labels.distinct.size != labels.size) invalid("options must be distinct")
      else if (labels.exists(_.trim.isEmpty)) invalid("options must not be blank")
      else Right(())
    checkInstructions.flatMap { _ =>
      question match {
        case Choice(_, options) =>
          for {
            _ <- checkLabels(options.map(_._1), maxChoiceOptions)
            _ <- options.traverse { case (label, description) => checkDescription(label, description) }
          } yield question
        case Score(_, options) => checkLabels(options, maxScoreLevels).map(_ => question)
        case YesNo(_, trueDescription, falseDescription) =>
          for {
            _ <- checkDescription("true", trueDescription)
            _ <- checkDescription("false", falseDescription)
          } yield question
      }
    }
  }

  def validateAll(questions: Map[String, Question]): Either[LlmError, Unit] =
    questions.toList.traverse { case (id, q) => validate(id, q) }.map(_ => ())

  /** The Cypher-facing shape: `{type, instructions, options?}`. */
  implicit val decoder: Decoder[Question] = Decoder.instance { (c: HCursor) =>
    def fail(message: String): DecodingFailure = DecodingFailure(message, c.downField("options").history)
    def labels(items: Vector[Json], kind: String): Decoder.Result[Vector[String]] =
      items.traverse(_.asString.toRight(fail(s"a $kind question's options list must hold strings")))
    for {
      typeName <- c.get[String]("type")
      // An explicit null passes here and fails validation with the instructions type message
      instructions <- c
        .downField("instructions")
        .focus
        .toRight(DecodingFailure("a question needs instructions", c.downField("instructions").history))
      question <- typeName match {
        case "yes-no" =>
          c.get[Option[Json]]("options").flatMap {
            case None => Right(YesNo(instructions))
            case Some(json) if json.isNull => Right(YesNo(instructions))
            case Some(json) =>
              for {
                obj <- json.asObject
                  .toRight(fail("a yes-no question's options must be a map describing true and false"))
                _ <- obj.keys
                  .find(key => key != "true" && key != "false")
                  .map { key =>
                    fail(s"""a yes-no question's options may only be "true" and "false", got "$key"""")
                  }
                  .toLeft(())
              } yield YesNo(instructions, obj("true").getOrElse(Json.Null), obj("false").getOrElse(Json.Null))
          }
        case "choice" =>
          c.get[Json]("options")
            .left
            .map(_ => fail("a choice question needs options"))
            .flatMap { json =>
              json.asArray
                .map(labels(_, "choice").map(_.map(_ -> Json.Null)))
                .orElse(json.asObject.map(obj => Right(obj.toVector)))
                .getOrElse(
                  Left(fail("a choice question's options must be a list of labels, or a map from label to description")),
                )
            }
            .map(Choice(instructions, _))
        case "score" =>
          c.get[Json]("options")
            .left
            .map(_ => fail("a score question needs options"))
            .flatMap(
              _.asArray.toRight(fail("a score question's options must be an ordered list, the order is the scale")),
            )
            .flatMap(labels(_, "score"))
            .map(Score(instructions, _))
        case other =>
          Left(DecodingFailure(s"""type must be one of $typeNames, got "$other"""", c.downField("type").history))
      }
    } yield question
  }
}

/** A decision model's answer to one question. `probabilities` is keyed by option label. */
sealed trait Answer extends Product with Serializable {
  def typeName: String
}

object Answer {
  final case class Choice(choice: String, probabilities: Map[String, Double], confidence: Option[Double])
      extends Answer {
    def typeName: String = "choice"
  }

  final case class Score(score: Double, probabilities: Map[String, Double], confidence: Option[Double]) extends Answer {
    def typeName: String = "score"
  }

  /** A yes-no answer is the truth probability alone. The api sends no confidence for one, the probability
    * already carries the uncertainty: `abs(2 * probability - 1)` is the equivalent on the choice scale.
    */
  final case class YesNo(probability: Double) extends Answer {
    def typeName: String = "yes-no"
  }

  /** The Cypher-facing shape: `type`, the answer field, and `probabilities` and `confidence` where present. */
  implicit val encoder: Encoder[Answer] = Encoder.instance { answer =>
    val rest = answer match {
      case Choice(choice, probabilities, confidence) =>
        confidence.toList.map("confidence" -> _.asJson) ++
          List("choice" -> choice.asJson, "probabilities" -> probabilities.asJson)
      case Score(score, probabilities, confidence) =>
        confidence.toList.map("confidence" -> _.asJson) ++
          List("score" -> score.asJson, "probabilities" -> probabilities.asJson)
      case YesNo(probability) => List("probability" -> probability.asJson)
    }
    Json.fromFields(("type" -> answer.typeName.asJson) :: rest)
  }
}

object Decision {

  /** A decision client: a state and typed questions in, a calibrated answer per question out. */
  type Decide = (Json, Map[String, Question]) => Future[Map[String, Answer]]

  /** Reject a state over the profile's `max-state-chars`, before any request leaves Quine; an unset limit
    * sends everything.
    */
  def checkSize(state: Json, limit: Option[Int]): Either[LlmError, Unit] =
    limit.fold[Either[LlmError, Unit]](Right(())) { max =>
      val chars = state.noSpaces.length
      Either.cond(chars <= max, (), LlmError.stateTooLarge(chars, max))
    }

  /** Reject answers that do not match the questions asked. Cypher reads a missing map key as null, so an unanswered
    * question would otherwise pass silently and send every threshold on it down the false branch.
    */
  def checkAnswers(
    questions: Map[String, Question],
    answers: Map[String, Answer],
  ): Either[LlmError, Map[String, Answer]] =
    (questions.keySet -- answers.keySet).toList.sorted.headOption
      .map(id => s"""no answer for question "$id"""")
      .orElse(
        (answers.keySet -- questions.keySet).toList.sorted.headOption
          .map(id => s"""an answer to "$id", a question that was not asked"""),
      )
      .orElse(
        questions.toList
          .sortBy(_._1)
          .collectFirst(Function.unlift { case (id, question) =>
            (question, answers(id)) match {
              case (q, a) if q.typeName != a.typeName =>
                Some(s"""the answer to "$id" is a ${a.typeName} answer to a ${q.typeName} question""")
              case (q: Question.Choice, a: Answer.Choice) if !q.labels.contains(a.choice) =>
                Some(s"""the answer to "$id" chose "${a.choice}", which is not one of its options""")
              case _ => None
            }
          }),
      )
      .map(LlmError.malformed(_))
      .toLeft(answers)
}
