package com.thatdot.quine.app.llm

import io.circe.Json
import io.circe.syntax._
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.common.security.Secret
import com.thatdot.quine.app.config.{LlmApi, LlmProfile}
import com.thatdot.quine.app.llm.LlmStubServer.openAiCompletion

/** `Completion.complete`: the composition of URI, headers, body, post, and parse for one profile. */
class CompletionTest extends AnyFunSuite with LlmTestSupport {

  private val openAi = LlmProfile.Completion(
    api = LlmApi.OpenAiChat(),
    baseUrl = "http://model.test/v1/",
    model = "test-model",
    apiKey = Some(Secret("sk-test")),
    systemPrompt = Some("Be terse."),
  )

  private def complete(stub: LlmStubPost): Completion.Complete = Completion.complete(openAi, stub.post)

  test("extra-body fields ride along, and a field the dialect sets itself keeps the dialect's value") {
    val stub = LlmStubPost.answering(openAiCompletion(Some("pong")))
    val thinking = openAi.copy(extraBody =
      Map(
        "reasoning_effort" -> "low".asJson,
        "chat_template_kwargs" -> Json.obj("enable_thinking" -> Json.False),
        "stream" -> Json.True,
      ),
    )
    await(Completion.complete(thinking, stub.post).apply(Prompt("ping")))
    val body = stub.received.loneElement.body
    assert(body.hcursor.get[String]("reasoning_effort") == Right("low"))
    assert(body.hcursor.downField("chat_template_kwargs").get[Boolean]("enable_thinking") == Right(false))
    assert(body.hcursor.get[Boolean]("stream") == Right(false), "stream stays off so the answer parses")
    assert(body.hcursor.get[String]("model") == Right("test-model"))
  }

  test("a prompt over max-prompt-chars fails before any request leaves, and an unset limit sends everything") {
    val stub = LlmStubPost.answering(openAiCompletion(Some("pong")))
    val prompt = Prompt("x" * 100)
    val limited = Completion.complete(openAi.copy(maxPromptChars = Some(40)), stub.post)
    val error = intercept[LlmError](await(limited(prompt)))
    // The profile's "Be terse." joins the user turn in the count: 100 + 9 = 109
    assert(error.getMessage == "prompt is 109 characters, over the profile limit of 40 (max-prompt-chars)")
    assert(!error.retryable)
    assert(stub.received.isEmpty, "the request never left")
    assert(
      await(complete(stub)(prompt)) == CompletionResult("pong", truncated = false),
      "no limit on the same prompt",
    )
  }
}
