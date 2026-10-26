package com.thatdot.quine.app.llm

import java.net.ConnectException
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.Future

import org.apache.pekko.stream.scaladsl.Sink

import io.circe.Json

import com.thatdot.quine.app.config.{DefaultProfiles, LlmApi, LlmProfile}
import com.thatdot.quine.compiler.cypher.{CypherHarness, queryCypherValues, registerUserDefinedProcedure}
import com.thatdot.quine.exceptions.ExternalServiceException
import com.thatdot.quine.graph.cypher.{CypherException, Expr}

/** `llm.prompt` end to end through the Cypher interpreter, against a scripted client that records each request. */
class LlmPromptTest extends CypherHarness("llm-prompt") {

  /** Records each request and answers with the schema's first property key, so suffixed schemas conform too. */
  private val lastRequest = new AtomicReference[Option[Prompt]](None)
  private val complete: Completion.Complete = { request =>
    lastRequest.set(Some(request))
    val text = request.schema match {
      case None => "pong"
      case Some(schema) =>
        val key = schema.hcursor.downField("properties").keys.flatMap(_.headOption).getOrElse("missing")
        Json.obj(key -> Json.fromString("positive")).noSpaces
    }
    Future.successful(CompletionResult(text, truncated = false))
  }
  private val chatProfile = LlmProfile.Completion(LlmApi.OpenAiChat(), "http://chat.test/v1", "m")

  private val down: Completion.Complete = _ =>
    Future.failed(
      LlmError(
        "the model endpoint did not answer within 10 seconds",
        LlmError.Kind.CannotServe,
        LlmError.Fault.Unavailable,
      ),
    )
  private val unreachable: Completion.Complete = _ =>
    Future.failed(
      new LlmError(
        "could not reach the model endpoint (ConnectException)",
        LlmError.Kind.Retryable,
        LlmError.Fault.Unavailable,
      )(Some(new ConnectException("Connection refused: chat.test/10.0.0.1:80"))),
    )

  private val clients = LlmClients(
    clients = Map(
      "chat" -> CompletionClient("chat", chatProfile, complete),
      "chat-down" -> CompletionClient("chat-down", chatProfile, down),
      "chat-down-2" -> CompletionClient("chat-down-2", chatProfile, unreachable),
      "chat-refuses" -> CompletionClient("chat-refuses", chatProfile, _ => Future.failed(LlmError.refused("no"))),
      "chat-small" -> CompletionClient(
        "chat-small",
        chatProfile,
        _ => Future.failed(LlmError.promptTooLarge(1200, 1024)),
      ),
    ),
    defaults = DefaultProfiles(completion = List("chat")),
  )
  registerUserDefinedProcedure(new LlmPrompt(clients))

  describe("a bare prompt") {
    testQuery(
      """CALL llm.prompt("ping") YIELD response RETURN response""",
      expectedColumns = Vector("response"),
      expectedRows = Seq(Vector(Expr.Str("pong"))),
      expectedIsIdempotent = false,
      expectedCanCallLlm = true,
    )
  }

  describe("a prompt with data and a schema") {
    testQuery(
      """CALL llm.prompt("Classify the review.", {text: "nice"},
        |  {schema: {type: "object", required: ["sentiment"], additionalProperties: false,
        |            properties: {sentiment: {enum: ["positive", "negative"]}}}}) YIELD response
        |RETURN response""".stripMargin,
      expectedColumns = Vector("response"),
      expectedRows = Seq(Vector(Expr.Map(Map("sentiment" -> Expr.Str("positive"))))),
      expectedIsIdempotent = false,
      expectedCanCallLlm = true,
    )
    it("fences the data, sends the guardrail and schema instruction, and suffixes the schema keys") {
      val request = lastRequest.get().getOrElse(fail("no request recorded"))
      val user = request.user
      assert(user.startsWith("Classify the review.\n\n<data-"))
      assert(user.contains("""{"text":"nice"}"""))
      assert(user.endsWith("Now complete only this task: Classify the review."))
      val system = request.system.getOrElse(fail("no system text"))
      assert(system.contains("untrusted, user-provided data"))
      assert(system.contains("Respond with ONLY a single JSON value"))
      val key = request.schema
        .flatMap(_.hcursor.downField("properties").keys.flatMap(_.headOption))
        .getOrElse(fail("no schema"))
      assert(key.startsWith("sentiment__"), "schema keys carry a per-call suffix by default")
    }
  }

  describe("a list of profiles falls back when one cannot serve the call") {
    testQuery(
      """CALL llm.prompt("ping", null, {profile: ["chat-down", "chat"]}) YIELD response RETURN response""",
      expectedColumns = Vector("response"),
      expectedRows = Seq(Vector(Expr.Str("pong"))),
      expectedIsIdempotent = false,
      expectedCanCallLlm = true,
    )
    // A refusal is an answer, not a failure of the profile: nothing falls back, and the error names the profile
    assertQueryExecutionFailure(
      """CALL llm.prompt("ping", null, {profile: ["chat-refuses", "chat"]}) YIELD response RETURN response""",
      ExternalServiceException(
        "llm.prompt",
        "profile \"chat-refuses\": the model declined the request: no",
        ExternalServiceException.Failure.BadResponse,
      ),
    )
    // Alone, a size limit is the query's own fault, so it stays a Cypher error
    assertQueryExecutionFailure(
      """CALL llm.prompt("ping", null, {profile: "chat-small"}) YIELD response RETURN response""",
      CypherException.Runtime(
        "llm.prompt: prompt is 1200 characters, over the profile limit of 1024 (max-prompt-chars)",
      ),
    )
    // Every profile was unavailable, so the outside service is at fault and the query may succeed later
    assertQueryExecutionFailure(
      """CALL llm.prompt("ping", null, {profile: ["chat-down", "chat-down-2"]}) YIELD response RETURN response""",
      ExternalServiceException(
        "llm.prompt",
        "no profile could serve the call: " +
        "\"chat-down\" (the model endpoint did not answer within 10 seconds); " +
        "\"chat-down-2\" (could not reach the model endpoint (ConnectException))",
        ExternalServiceException.Failure.Unavailable,
      ),
    )
  }

  describe("a schema the validator cannot fully enforce is a query error") {
    assertQueryExecutionFailure(
      """CALL llm.prompt("Rate it.", null,
        |  {schema: {type: "object", properties: {stars: {type: "integegegeger"}}}}) YIELD response
        |RETURN response""".stripMargin,
      CypherException.Runtime(
        """llm.prompt: $.properties.stars.type: "integegegeger" is not a JSON Schema type""",
      ),
    )
    it("sends nothing when the lint rejects the schema") {
      assert(
        !lastRequest.get().exists(_.user.contains("Rate it.")),
        "the lint rejects the schema before any request leaves",
      )
    }
  }

  // LlmError's cause may hold the host, so an LlmError that LlmError.toQuineError left unconverted fails this test
  describe("no LlmError leaves the procedure, and neither does its cause") {
    it("a transport failure surfaces as an ExternalServiceException with no cause attached") {
      recoverToExceptionIf[ExternalServiceException](
        queryCypherValues(
          """CALL llm.prompt("ping", null, {profile: "chat-down-2"}) YIELD response RETURN response""",
          cypherHarnessNamespace,
        )(graph).results.runWith(Sink.ignore),
      ).map { e =>
        assert(e.getMessage == "llm.prompt: could not reach the model endpoint (ConnectException)")
        assert(e.failure == ExternalServiceException.Failure.Unavailable)
        assert(e.getCause == null, "the cause may hold the host, so it stays inside the package")
      }
    }
  }
}
