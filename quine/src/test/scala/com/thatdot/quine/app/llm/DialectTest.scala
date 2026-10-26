package com.thatdot.quine.app.llm

import org.apache.pekko.http.scaladsl.model.HttpHeader

import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax._
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.common.security.Secret
import com.thatdot.quine.app.config.LlmApi
import com.thatdot.quine.app.llm.Completion.{BodySettings, anthropicMessages, openaiChat}
import com.thatdot.quine.app.llm.LlmStubServer.{anthropicCompletion, openAiCompletion}

/** The two dialects as pure functions: the request each one builds and how each one reads the answer. */
class DialectTest extends AnyFunSuite {

  private val openAiApi = LlmApi.OpenAiChat(temperature = Some(0.0))
  private val openAiDialect = openaiChat(openAiApi)
  private val openAiKey = Some(Secret("sk-test"))
  private val openAiSettings = BodySettings("test-model", maxTokens = 4096, systemPrompt = Some("Be terse."))
  private val anthropicKey = Some(Secret("ant-key"))
  private val anthropicSettings = BodySettings("claude-test", maxTokens = 512, systemPrompt = Some("Be terse."))
  private val schema: Json = parse("""{"type":"object","properties":{"k":{"type":"string"}}}""").fold(throw _, identity)

  private def header(headers: List[HttpHeader], name: String): Option[String] =
    headers.find(_.is(name.toLowerCase)).map(_.value)

  test("openai-chat request: path, bearer header, system joined, sampling, native structured output") {
    assert(openAiDialect.path == "/chat/completions")
    assert(header(openAiDialect.headers(openAiKey), "Authorization").contains("Bearer sk-test"))
    val body = openAiDialect.body(openAiSettings)(Prompt("ping", system = Some("Extra."), schema = Some(schema)))
    assert(
      body == Json.obj(
        "model" -> "test-model".asJson,
        "messages" -> Json.arr(
          Json.obj("role" -> "system".asJson, "content" -> "Be terse.\n\nExtra.".asJson),
          Json.obj("role" -> "user".asJson, "content" -> "ping".asJson),
        ),
        "stream" -> Json.False,
        "max_tokens" -> 4096.asJson,
        "temperature" -> 0.0.asJson,
        "response_format" -> Json.obj(
          "type" -> "json_schema".asJson,
          "json_schema" -> Json.obj("name" -> "output".asJson, "schema" -> schema, "strict" -> Json.True),
        ),
      ),
    )
  }

  test("openai-chat response: length is truncated; null content with a refusal and content_filter are refused") {
    assert(openAiDialect.parse(openAiCompletion(Some("pong"))) == Right(CompletionResult("pong", truncated = false)))
    assert(
      openAiDialect.parse(openAiCompletion(Some("partial"), finishReason = "length")) ==
        Right(CompletionResult("partial", truncated = true)),
    )
    assert(
      openAiDialect.parse(openAiCompletion(None, refusal = Some("I will not"))).left.map(_.getMessage) ==
        Left("the model declined the request: I will not"),
    )
    assert(
      openAiDialect
        .parse(openAiCompletion(Some("[filtered]"), finishReason = "content_filter"))
        .left
        .map(_.getMessage) ==
        Left("the model declined the request: finish_reason was content_filter"),
    )
  }

  test("anthropic-messages request: path, api key and version headers, system joined, no temperature") {
    assert(anthropicMessages.path == "/v1/messages")
    val headers = anthropicMessages.headers(anthropicKey)
    assert(header(headers, "x-api-key").contains("ant-key"))
    assert(header(headers, "anthropic-version").contains("2023-06-01"))
    assert(header(headers, "Authorization").isEmpty)
    val body =
      anthropicMessages.body(anthropicSettings)(Prompt("ping", system = Some("Extra."), schema = Some(schema)))
    assert(
      body == Json.obj(
        "model" -> "claude-test".asJson,
        "max_tokens" -> 512.asJson,
        "messages" -> Json.arr(Json.obj("role" -> "user".asJson, "content" -> "ping".asJson)),
        "system" -> "Be terse.\n\nExtra.".asJson,
      ),
      "the Messages API has no temperature here and no native structured output",
    )
  }

  test("anthropic-messages response: text blocks are joined; max_tokens is truncated; refusal is refused") {
    assert(
      anthropicMessages.parse(anthropicCompletion(List("a", "b"), stopReason = "max_tokens")) ==
        Right(CompletionResult("a\nb", truncated = true)),
    )
    assert(
      anthropicMessages.parse(anthropicCompletion(List("pong"))) == Right(CompletionResult("pong", truncated = false)),
    )
    assert(
      anthropicMessages.parse(anthropicCompletion(Nil, stopReason = "refusal")).left.map(_.getMessage) ==
        Left("the model declined the request: stop_reason was refusal"),
    )
  }
}
