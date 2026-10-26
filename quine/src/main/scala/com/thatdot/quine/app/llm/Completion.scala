package com.thatdot.quine.app.llm

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.http.scaladsl.model.HttpHeader
import org.apache.pekko.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken, RawHeader}

import io.circe.syntax._
import io.circe.{ACursor, Json}

import com.thatdot.common.security.Secret
import com.thatdot.quine.app.config.{LlmApi, LlmProfile}
import com.thatdot.quine.app.llm.LlmHttp.Post

/** What a caller wants answered.
  * @param user the user turn: the prompt, with any fenced data
  * @param system system text for this request, appended after the profile's own system prompt
  * @param schema JSON schema the answer should conform to; sent to the server when the profile supports native
  *               structured output, otherwise only a hint for the caller's own validation
  */
final case class Prompt(user: String, system: Option[String] = None, schema: Option[Json] = None)

/** A completion model's answer to one [[Prompt]].
  * @param truncated the model stopped at its token limit, so `text` may be cut off
  */
final case class CompletionResult(text: String, truncated: Boolean)

/** Completion: a prompt in, a [[CompletionResult]] out. Everything api-specific is a pure function on a
  * [[Completion.Dialect]].
  */
object Completion {

  /** A completion client. A lambda is a client; a test fake is a lambda. */
  type Complete = Prompt => Future[CompletionResult]

  /** The profile fields a request body uses. The api is not among them, so a body call cannot carry an api
    * that disagrees with the one its dialect closed over.
    */
  final case class BodySettings(model: String, maxTokens: Int, systemPrompt: Option[String])

  /** One chat api's wire format as four pure functions, closed over that api's own settings. */
  final case class Dialect(
    path: String,
    headers: Option[Secret] => List[HttpHeader],
    body: BodySettings => Prompt => Json,
    parse: Json => Either[LlmError, CompletionResult],
  )

  private def joinSystem(systemPrompt: Option[String], prompt: Prompt): Option[String] =
    (systemPrompt.toList ++ prompt.system.toList) match {
      case Nil => None
      case parts => Some(parts.mkString("\n\n"))
    }

  private def userMessage(prompt: Prompt): Json =
    Json.obj("role" -> "user".asJson, "content" -> prompt.user.asJson)

  private def bearer(apiKey: Option[Secret]): List[HttpHeader] = {
    import Secret.Unsafe._
    apiKey.map(key => Authorization(OAuth2BearerToken(key.unsafeValue))).toList
  }

  /** OpenAI chat completions; Ollama, vLLM, LM Studio, and llama.cpp serve the same shape. The dialect closes
    * over the api's own settings: temperature, the token-cap field spelling, and native structured output.
    */
  def openaiChat(api: LlmApi.OpenAiChat): Dialect = Dialect(
    path = "/chat/completions",
    headers = bearer,
    body = settings =>
      prompt => {
        val messages: List[Json] =
          joinSystem(settings.systemPrompt, prompt)
            .map(s => Json.obj("role" -> "system".asJson, "content" -> s.asJson))
            .toList :+
          userMessage(prompt)
        val responseFormat: Option[(String, Json)] =
          if (api.nativeStructuredOutput)
            prompt.schema.map { schema =>
              "response_format" -> Json.obj(
                "type" -> "json_schema".asJson,
                "json_schema" -> Json.obj("name" -> "output".asJson, "schema" -> schema, "strict" -> Json.True),
              )
            }
          else None
        Json.fromFields(
          List(
            "model" -> settings.model.asJson,
            "messages" -> Json.arr(messages: _*),
            "stream" -> Json.False,
            api.maxTokensField.fieldName -> settings.maxTokens.asJson,
          ) ++ api.temperature.map("temperature" -> _.asJson) ++ responseFormat,
        )
      },
    parse = json => {
      val choice: ACursor = json.hcursor.downField("choices").downArray
      val message: ACursor = choice.downField("message")
      val finishReason: Option[String] = choice.get[String]("finish_reason").toOption
      // A failed cursor decodes as None, so inspect the focus to tell missing content from an explicit null
      (finishReason, message.downField("content").focus) match {
        case (Some("content_filter"), _) => Left(LlmError.refused("finish_reason was content_filter"))
        case (_, None) => Left(LlmError.malformed("no choices[0].message.content"))
        case (_, Some(content)) if content.isNull =>
          Left(LlmError.refused(message.get[String]("refusal").getOrElse("the response had no content")))
        case (_, Some(content)) =>
          content.asString
            .map(text => CompletionResult(text, truncated = finishReason.contains("length")))
            .toRight(LlmError.malformed("choices[0].message.content is not a string"))
      }
    },
  )

  /** The Anthropic Messages API. Structured output rides in the system text; `temperature` is never sent. */
  val anthropicMessages: Dialect = Dialect(
    path = "/v1/messages",
    headers = apiKey => {
      import Secret.Unsafe._
      RawHeader("anthropic-version", "2023-06-01") ::
      apiKey.map(key => RawHeader("x-api-key", key.unsafeValue)).toList
    },
    body = settings =>
      prompt =>
        Json.fromFields(
          List(
            "model" -> settings.model.asJson,
            "max_tokens" -> settings.maxTokens.asJson,
            "messages" -> Json.arr(userMessage(prompt)),
          ) ++ joinSystem(settings.systemPrompt, prompt).map("system" -> _.asJson),
        ),
    parse = json => {
      val cursor = json.hcursor
      val stopReason: Option[String] = cursor.get[String]("stop_reason").toOption
      val textBlocks: Vector[String] = cursor
        .downField("content")
        .as[Vector[Json]]
        .toOption
        .toVector
        .flatten
        .flatMap(block => block.hcursor.get[String]("text").toOption)
      if (stopReason.contains("refusal")) Left(LlmError.refused("stop_reason was refusal"))
      else if (textBlocks.isEmpty) Left(LlmError.malformed("no text block in content"))
      else Right(CompletionResult(textBlocks.mkString("\n"), truncated = stopReason.contains("max_tokens")))
    },
  )

  def dialectOf(api: LlmApi.CompletionApi): Dialect = api match {
    case openai: LlmApi.OpenAiChat => openaiChat(openai)
    case LlmApi.AnthropicMessages => anthropicMessages
  }

  /** Reject a prompt over the profile's `max-prompt-chars`, before any request leaves Quine; an unset limit
    * sends everything. The count covers the user turn, with any fenced data, and the system text.
    */
  private def checkSize(profile: LlmProfile.Completion, prompt: Prompt): Either[LlmError, Unit] =
    profile.maxPromptChars.fold[Either[LlmError, Unit]](Right(())) { limit =>
      val chars = prompt.user.length + joinSystem(profile.systemPrompt, prompt).fold(0)(_.length)
      Either.cond(chars <= limit, (), LlmError.promptTooLarge(chars, limit))
    }

  /** Build the client for one profile: fix the URI, headers, and dialect once, then every call is check, body,
    * post, parse.
    */
  def complete(profile: LlmProfile.Completion, post: Post): Complete = {
    val dialect = dialectOf(profile.api)
    val uri = profile.baseUrl.stripSuffix("/") + dialect.path
    val headers = dialect.headers(profile.apiKey)
    val body = dialect.body(BodySettings(profile.model, profile.maxTokens, profile.systemPrompt))
    // The profile's extra fields ride along as they are; a field the dialect sets itself keeps the dialect's value
    val extra = Json.fromFields(profile.extraBody)
    prompt =>
      Future
        .fromTry(checkSize(profile, prompt).toTry)
        // Both continuations are CPU-only and non-blocking, so neither needs a dispatcher hop: the first
        // renders the payload and offers it to the pool queue, the second walks an already-arrived response
        .flatMap(_ => post(uri, headers, extra.deepMerge(body(prompt))))(ExecutionContext.parasitic)
        .flatMap(json => Future.fromTry(dialect.parse(json).toTry))(ExecutionContext.parasitic)
  }
}
