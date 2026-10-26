package com.thatdot.quine.app.llm

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import cats.syntax.applicativeError._
import io.circe.Json

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
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

/** `llm.prompt(prompt, data?, options?)`, as `description` says. Options: `profile`, `schema`, `randomizeSchemaKeys`.
  * `profile` is a profile name, or a list of names tried in order when a profile cannot serve the call.
  */
final class LlmPrompt(clients: LlmClients) extends UserDefinedProcedure with LazySafeLogging {
  val name = "llm.prompt"
  val canContainUpdates = false
  val isIdempotent = false
  val canContainAllNodeScan = false
  override val canCallLlm = true

  val signature: UserDefinedProcedureSignature = UserDefinedProcedureSignature(
    arguments = Vector("prompt" -> Type.Str, "data" -> Type.Anything, "options" -> Type.Map),
    outputs = Vector("response" -> Type.Anything),
    description =
      "Sends a prompt, with optional fenced untrusted data and an expected-output JSON schema, to a configured " +
      "completion model and yields the completion text, or the schema-validated value, or null when the " +
      "answer does not conform to the schema. The response is untrusted output.",
  )

  private val allowedOptions = Set("profile", "schema", "randomizeSchemaKeys")

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

    val (prompt, dataOrNull, optionsMap): (String, Option[Value], Map[String, Value]) = arguments match {
      case Seq(Expr.Str(prompt)) => (prompt, None, Map.empty)
      case Seq(Expr.Str(prompt), data) => (prompt, Some(data), Map.empty)
      case Seq(Expr.Str(prompt), data, Expr.Map(options)) => (prompt, Some(data), options)
      case Seq(Expr.Str(prompt), data, Expr.Null) => (prompt, Some(data), Map.empty)
      case other => throw wrongSignature(other)
    }
    // Null in an optional position means the same as omitting it, the history.* convention
    val data: Option[Json] = dataOrNull.filter(_ != Expr.Null).map(Value.toJson)
    val options = new LlmProcedureOptions(name, optionsMap, allowedOptions)
    options.rejectUnknown()

    val chain: List[CompletionClient] = options.orThrow(clients.completionChain(options.stringOrList("profile")))

    val schema: Option[Json] = options.json("schema")

    // A schema the validator would not fully enforce is the query's own fault: fail before any request leaves
    schema.map(JsonSchemaValidator.checkSchema(_)).filter(_.nonEmpty).foreach { problems =>
      throw options.error(problems.mkString(", and "))
    }

    // The randomizeSchemaKeys default is the profile's, and the key suffix and fence nonce are fresh per
    // request, so each client the fallback tries gets a request built for it
    def askClient(client: CompletionClient): Future[Vector[Value]] = {
      val randomizeKeys: Boolean =
        options.boolean("randomizeSchemaKeys").getOrElse(client.profile.randomizeSchemaKeys)
      val keySuffix: Option[String] = schema.filter(_ => randomizeKeys).map(_ => PromptFencing.newKeySuffix())
      val effectiveSchema: Option[Json] = schema.map(s => keySuffix.fold(s)(PromptFencing.suffixSchemaKeys(s, _)))
      val schemaInstruction: Option[String] = effectiveSchema.map(PromptFencing.schemaInstruction(_, keySuffix))

      val (guardrail, userContent): (Option[String], String) = data match {
        case None => (None, prompt)
        case Some(json) =>
          val fenced = PromptFencing.fence(prompt, json, PromptFencing.newNonce())
          (Some(fenced.guardrail), fenced.userContent)
      }
      val system: Option[String] = (guardrail.toList ++ schemaInstruction.toList) match {
        case Nil => None
        case parts => Some(parts.mkString("\n\n"))
      }
      val request = Prompt(user = userContent, system = system, schema = effectiveSchema)
      client.complete(request).map { completion =>
        if (completion.truncated)
          logger.debug(log"${Safe(name)} output from profile ${Safe(client.name)} stopped at the model's token limit")
        effectiveSchema match {
          case None => Vector(Expr.Str(completion.text))
          case Some(schemaUsed) =>
            PromptFencing.parseJsonLoose(completion.text) match {
              case Some(parsed) if JsonSchemaValidator.isValid(schemaUsed, parsed) =>
                Vector(Value.fromJson(keySuffix.fold(parsed)(PromptFencing.stripKeySuffix(parsed, _))))
              case _ =>
                logger.debug(
                  log"${Safe(name)} output from profile ${Safe(client.name)} did not conform to the schema, yielding null",
                )
                Vector(Expr.Null)
            }
        }
      }
    }

    Source.future(LlmFallback.tryEach(name, chain)(askClient).adaptErr(LlmError.toQuineError(name)))
  }
}
