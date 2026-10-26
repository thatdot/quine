package com.thatdot.quine.app.config

import scala.concurrent.duration.DurationInt

import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite
import pureconfig.error.ConfigReaderException
import pureconfig.{ConfigSource, ConfigWriter}

import com.thatdot.common.security.Secret

class LlmConfigTest extends AnyFunSuite {

  private def read(hocon: String): LlmConfig = ConfigSource.string(hocon).loadOrThrow[LlmConfig]
  private def failure(hocon: String): String =
    intercept[ConfigReaderException[LlmConfig]](read(hocon)).failures.prettyPrint()

  /** One profile named `p` with the least a profile needs; `extra` adds keys after `model`, with its own comma. */
  private def profileWith(api: String = "openai-chat", baseUrl: String = "http://h/v1", extra: String = ""): String =
    s"""profiles { p { api { type = $api }, base-url = "$baseUrl", model = "m"$extra } }"""

  private val both =
    """default-profiles { completion = "qwen-local", decision = "jev-local" }
      |profiles {
      |  qwen-local {
      |    api { type = openai-chat, temperature = 0.2, native-structured-output = false }
      |    base-url = "http://localhost:11434/v1"
      |    model = "qwen3.5:8b"
      |    api-key = "sk-local"
      |    max-prompt-chars = 32768
      |    system-prompt = "Be terse."
      |    max-concurrent-requests = 4
      |    request-timeout = 30s
      |    extra-body { reasoning_effort = "low", chat_template_kwargs { enable_thinking = false } }
      |  }
      |  claude {
      |    api { type = anthropic-messages }
      |    base-url = "https://api.anthropic.com"
      |    model = "claude-sonnet-5"
      |  }
      |  jev-local { api { type = jev-systemone }, base-url = "http://localhost:8000", model = "Qwen/Qwen3.5-0.8B", max-state-chars = 2048 }
      |}""".stripMargin

  test("the api object alone selects the profile kind, and defaults fill in") {
    val config = read(both)
    assert(
      config.defaultProfiles == DefaultProfiles(List("qwen-local"), List("jev-local")),
      "a bare string reads as a one-element chain",
    )
    assert(
      config.profiles("qwen-local") == LlmProfile.Completion(
        api = LlmApi.OpenAiChat(temperature = Some(0.2), nativeStructuredOutput = false),
        baseUrl = "http://localhost:11434/v1",
        model = "qwen3.5:8b",
        apiKey = Some(Secret("sk-local")),
        maxPromptChars = Some(32768),
        systemPrompt = Some("Be terse."),
        maxConcurrentRequests = 4,
        requestTimeout = 30.seconds,
        extraBody = Map(
          "reasoning_effort" -> Json.fromString("low"),
          "chat_template_kwargs" -> Json.obj("enable_thinking" -> Json.False),
        ),
      ),
    )
    assert(
      config.profiles("claude") == LlmProfile.Completion(
        api = LlmApi.AnthropicMessages,
        baseUrl = "https://api.anthropic.com",
        model = "claude-sonnet-5",
      ),
    )
    assert(
      config.profiles("jev-local") == LlmProfile.Decision(
        api = LlmApi.JevSystemOne,
        baseUrl = "http://localhost:8000",
        model = "Qwen/Qwen3.5-0.8B",
        maxStateChars = Some(2048),
      ),
    )
  }

  test("writing a config redacts api keys and round-trips") {
    val config = read(both)
    val rendered = ConfigWriter[LlmConfig].to(config).render()
    assert(!rendered.contains("sk-local"))
    assert(rendered.contains("Secret(****)"))
    assert(rendered.contains("anthropic-messages") && rendered.contains("jev-systemone"))
    val redacted = config.copy(profiles = config.profiles.map {
      case (name, c: LlmProfile.Completion) => name -> c.copy(apiKey = c.apiKey.map(_ => Secret("Secret(****)")))
      case (name, d: LlmProfile.Decision) => name -> d.copy(apiKey = d.apiKey.map(_ => Secret("Secret(****)")))
    })
    assert(ConfigSource.string(rendered).loadOrThrow[LlmConfig] == redacted)
  }

  test("a default must name a profile of its kind, and every name in a chain is checked") {
    val profiles = profileWith() // a completion profile named p
    assert(
      failure(s"""default-profiles { completion = ["p", "missing"] }\n$profiles""").contains(
        "default-profiles.completion names profile \"missing\", which is not defined under profiles",
      ),
      "every name in the chain is checked, not only the first",
    )
    assert(
      failure(s"""default-profiles { decision = "p" }\n$profiles""")
        .contains("default-profiles.decision names profile \"p\", which has kind completion"),
    )
  }

  test("a config with several faults reports every fault in one error") {
    val error = failure(s"""default-profiles { completion = ["p", "missing"], decision = "p" }\n${profileWith()}""")
    assert(
      error.contains(
        "default-profiles.completion names profile \"missing\", which is not defined under profiles, " +
        "and default-profiles.decision names profile \"p\", which has kind completion",
      ),
      "one load reports every fault, joined in check order",
    )
  }

  test("a missing or unknown api, and unknown keys, are rejected") {
    assert(failure("""profiles { p { base-url = "http://h", model = "m" } }""").contains("api"))
    assert(failure(profileWith("gemini")).contains("gemini"))
    assert(
      failure(profileWith("jev-systemone", extra = ", temperature = 0.1")).contains("temperature"),
      "an api's own settings live in the api object, so temperature at the profile level is unknown",
    )
  }

  test("a request-timeout the scheduler could not time is rejected at load, on its key") {
    // A delay beyond the scheduler's wheel throws at the first call and kills the profile's pool for
    // the life of the process, so the bound is enforced here, where the message names the config key
    assert(
      failure(profileWith(extra = ", request-timeout = 300d"))
        .contains("request-timeout must be at most 24 days, the scheduler cannot time more"),
    )
    assert(
      read(profileWith(extra = ", request-timeout = 24d")).profiles("p").requestTimeout == 24.days,
      "the ceiling itself is representable on every scheduler configuration, so it is accepted",
    )
  }

  test("the llm block parses inside the full Quine config") {
    val quine = ConfigSource.string(s"quine { llm { $both } }").loadOrThrow[QuineConfig]
    assert(quine.llm.profiles.keySet == Set("qwen-local", "claude", "jev-local"))
  }
}
