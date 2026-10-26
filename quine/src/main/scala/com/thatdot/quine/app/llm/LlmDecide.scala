package com.thatdot.quine.app.llm

import scala.concurrent.ExecutionContext

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import cats.syntax.applicativeError._
import io.circe.syntax._

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.graph.cypher.{
  Expr,
  Parameters,
  ProcedureExecutionLocation,
  QueryContext,
  Type,
  UserDefinedProcedure,
  UserDefinedProcedureSignature,
  Value,
}

/** `llm.decide(state, questions, options?)`; the answers are as `description` says, thresholds are the query's
  * business. Option `profile`: a profile name, or a list of names tried in order when a profile cannot serve the call.
  */
final class LlmDecide(clients: LlmClients) extends UserDefinedProcedure {
  val name = "llm.decide"
  val canContainUpdates = false
  val isIdempotent = false
  val canContainAllNodeScan = false
  override val canCallLlm = true

  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("state" -> Type.Anything, "questions" -> Type.Map, "options" -> Type.Map),
    outputs = Vector("answers" -> Type.Map),
    description =
      "Sends a state and a map of typed questions (yes-no, choice, or score) to a configured decision model and " +
      "yields a map from question id to the model's answer, with probabilities and confidence on choice and " +
      "score answers; a yes-no answer is a probability of yes.",
  )

  private val allowedOptions = Set("profile")

  def call(
    context: QueryContext,
    arguments: Seq[Value],
    location: ProcedureExecutionLocation,
  )(implicit
    parameters: Parameters,
    timeout: Timeout,
    logConfig: LogConfig,
  ): Source[Vector[Value], _] = {
    implicit val ec: ExecutionContext = location.graph.system.dispatcher
    import location.idProvider

    val (state, questionValues, optionsMap): (Value, Map[String, Value], Map[String, Value]) = arguments match {
      case Seq(state, Expr.Map(questions)) => (state, questions, Map.empty)
      case Seq(state, Expr.Map(questions), Expr.Map(options)) => (state, questions, options)
      // Null options mean the same as omitting them, the history.* convention
      case Seq(state, Expr.Map(questions), Expr.Null) => (state, questions, Map.empty)
      case other => throw wrongSignature(other)
    }
    val options = new LlmProcedureOptions(name, optionsMap, allowedOptions)
    options.rejectUnknown()

    val chain: List[DecisionClient] = options.orThrow(clients.decisionChain(options.stringOrList("profile")))

    val questions: Map[String, Question] = questionValues.map {
      case (id, fields @ Expr.Map(_)) =>
        Value.toJson(fields).as[Question] match {
          case Right(question) => id -> question
          case Left(failure) => throw options.error(s"""question "$id": ${failure.message}""")
        }
      case (id, _) => throw options.error(s"""question "$id": must be a map with type and instructions""")
    }

    val stateJson = Value.toJson(state)

    Source.future(
      LlmFallback
        .tryEach(name, chain)(client => client.decide(stateJson, questions))
        .adaptErr(LlmError.toQuineError(name))
        .map(answers => Vector(Expr.Map(answers.map { case (id, answer) => id -> Value.fromJson(answer.asJson) }))),
    )
  }
}
