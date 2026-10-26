package com.thatdot.quine.app.config

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Try

import org.apache.pekko.http.scaladsl.model.Uri

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValue}
import io.circe.Json
import io.circe.parser.parse
import pureconfig.error.{CannotConvert, UserValidationFailed}
import pureconfig.generic.FieldCoproductHint
import pureconfig.generic.semiauto.deriveConvert
import pureconfig.{ConfigConvert, ConfigReader, ConfigWriter}

import com.thatdot.common.security.Secret

/** The wire format an endpoint speaks, holding the settings only that api takes. The completion and decision
  * subtrees keep a client builder from receiving an api of the other kind. In config: `api { type = openai-chat }`.
  */
sealed trait LlmApi extends Product with Serializable {

  /** "completion" or "decision", for messages; the types carry the side everywhere else. */
  def kindName: String
}

object LlmApi extends PureconfigInstances {

  /** The side names, for code that holds no api: messages that name the side a call expects. */
  val completionKindName: String = "completion"
  val decisionKindName: String = "decision"

  /** An api [[LlmProfile.Completion]] may name. */
  sealed trait CompletionApi extends LlmApi {
    final def kindName: String = completionKindName
  }

  /** An api [[LlmProfile.Decision]] may name. */
  sealed trait DecisionApi extends LlmApi {
    final def kindName: String = decisionKindName
  }

  /** The request field for `max-tokens` on `openai-chat`; servers disagree on its name, so the operator picks. */
  sealed trait MaxTokensField extends Product with Serializable {
    def fieldName: String
  }

  object MaxTokensField {

    /** `max_tokens`, the spelling OpenAI-compatible local servers accept. */
    case object MaxTokens extends MaxTokensField { val fieldName = "max_tokens" }

    /** `max_completion_tokens`, the spelling `api.openai.com` requires for its current models. */
    case object MaxCompletionTokens extends MaxTokensField { val fieldName = "max_completion_tokens" }

    val all: List[MaxTokensField] = List(MaxTokens, MaxCompletionTokens)

    implicit val configReader: ConfigReader[MaxTokensField] = ConfigReader.fromString { str =>
      all
        .find(_.fieldName == str.toLowerCase)
        .toRight(CannotConvert(str, "MaxTokensField", s"must be one of ${all.map(_.fieldName).mkString(", ")}"))
    }

    implicit val configWriter: ConfigWriter[MaxTokensField] = ConfigWriter.toString(_.fieldName)
  }

  /** OpenAI chat completions, `POST {base-url}/chat/completions`; Ollama, vLLM, LM Studio, and llama.cpp
    * speak the same shape.
    *
    * @param temperature sampling temperature; the other apis take none
    * @param maxTokensField which request field carries the profile's `max-tokens`, see [[MaxTokensField]]
    * @param nativeStructuredOutput send a caller's JSON schema as `response_format`
    */
  final case class OpenAiChat(
    temperature: Option[Double] = None,
    maxTokensField: MaxTokensField = MaxTokensField.MaxTokens,
    nativeStructuredOutput: Boolean = true,
  ) extends CompletionApi

  /** The Anthropic Messages API, `POST {base-url}/v1/messages`; no `temperature`, current Claude models reject it. */
  case object AnthropicMessages extends CompletionApi

  /** The Jev decision API, `POST {base-url}/v1/systemone` */
  case object JevSystemOne extends DecisionApi

  /** `type` picks the api; the mapping is explicit because the derived default would kebab-case `OpenAiChat`
    * to `open-ai-chat` where the wire name is `openai-chat`.
    */
  private def hint[T <: LlmApi]: FieldCoproductHint[T] = new FieldCoproductHint[T]("type") {
    override protected def fieldValue(name: String): String = name match {
      case "OpenAiChat" => "openai-chat"
      case "AnthropicMessages" => "anthropic-messages"
      case "JevSystemOne" => "jev-systemone"
      case other => super.fieldValue(other)
    }
  }

  implicit val llmApiHint: FieldCoproductHint[LlmApi] = hint
  implicit val completionApiHint: FieldCoproductHint[CompletionApi] = hint
  implicit val decisionApiHint: FieldCoproductHint[DecisionApi] = hint

  // Sealed-family derivation is not recursive: each member needs its own convert in scope first
  implicit val openAiChatConvert: ConfigConvert[OpenAiChat] = deriveConvert[OpenAiChat]
  implicit val anthropicMessagesConvert: ConfigConvert[AnthropicMessages.type] = deriveConvert[AnthropicMessages.type]
  implicit val jevSystemOneConvert: ConfigConvert[JevSystemOne.type] = deriveConvert[JevSystemOne.type]

  implicit val completionApiConvert: ConfigConvert[CompletionApi] = deriveConvert[CompletionApi]
  implicit val decisionApiConvert: ConfigConvert[DecisionApi] = deriveConvert[DecisionApi]
  implicit val configConvert: ConfigConvert[LlmApi] = deriveConvert[LlmApi]
}

/** One entry under `llm.profiles`; `api` picks the variant, and each variant accepts only its own fields.
  * A profile has no side name of its own: ask its api (`profile.api.kindName`).
  */
sealed trait LlmProfile extends Product with Serializable {
  def api: LlmApi
  def baseUrl: String
  def model: String
  def apiKey: Option[Secret]
  def maxConcurrentRequests: Int
  def requestTimeout: FiniteDuration
}

object LlmProfile extends PureconfigInstances {

  /** A HOCON value as the JSON it stands for, for fields passed to a server as they are. */
  implicit val jsonConvert: ConfigConvert[Json] = ConfigConvert.fromReaderAndWriter(
    ConfigReader[ConfigValue].emap { value =>
      parse(value.render(ConfigRenderOptions.concise())).left
        .map(e => CannotConvert(value.render(), "Json", e.getMessage))
    },
    ConfigWriter.fromFunction(json => ConfigFactory.parseString(s"value = ${json.noSpaces}").getValue("value")),
  )

  /** A chat model reached through an OpenAI-compatible or Anthropic endpoint.
    *
    * @param api which wire format the endpoint speaks, with that api's own settings inside it
    * @param baseUrl endpoint base, including the `/v1` suffix for OpenAI-compatible servers
    * @param model model identifier sent with every request
    * @param apiKey bearer token (OpenAI dialect) or `x-api-key` (Anthropic); none for local servers
    * @param maxTokens maximum number of tokens the model may generate per request
    * @param maxPromptChars largest prompt one call may send, user turn plus system text, counted as
    *                       `String.length`, so in UTF-16 code units: a character outside the basic plane,
    *                       such as an emoji, counts as 2. Unset means no limit
    * @param systemPrompt fixed system text prepended to every request
    * @param maxConcurrentRequests requests in flight at once for this profile; further calls queue
    * @param requestTimeout how long one request may take before it fails
    * @param randomizeSchemaKeys default for the `randomizeSchemaKeys` option of `llm.prompt`
    * @param extraBody fields added to every request body as they are, for settings a server defines beyond the
    *                  api, such as `reasoning_effort`, `think`, or `chat_template_kwargs` for thinking models.
    *                  A field the dialect sets itself, such as `stream`, keeps the dialect's value.
    */
  final case class Completion(
    api: LlmApi.CompletionApi,
    baseUrl: String,
    model: String,
    apiKey: Option[Secret] = None,
    maxTokens: Int = 4096,
    maxPromptChars: Option[Int] = None,
    systemPrompt: Option[String] = None,
    maxConcurrentRequests: Int = 8,
    requestTimeout: FiniteDuration = 60.seconds,
    randomizeSchemaKeys: Boolean = true,
    extraBody: Map[String, Json] = Map.empty,
  ) extends LlmProfile

  /** A decision model: typed questions about a state in, calibrated answers out.
    *
    * @param api which wire format the endpoint speaks; it also fixes the request path under `baseUrl`
    * @param baseUrl endpoint base, without the api's own path
    * @param model model identifier sent with every request
    * @param apiKey bearer token; none for local servers
    * @param maxStateChars largest state one call may send, as serialized JSON counted in UTF-16 code units
    *                      (`String.length`); unset means no local limit. Jev itself takes up to 32k state
    *                      tokens, then answers 400
    * @param maxConcurrentRequests requests in flight at once for this profile; further calls queue
    * @param requestTimeout how long one request may take before it fails
    */
  final case class Decision(
    api: LlmApi.DecisionApi,
    baseUrl: String,
    model: String,
    apiKey: Option[Secret] = None,
    maxStateChars: Option[Int] = None,
    maxConcurrentRequests: Int = 16,
    requestTimeout: FiniteDuration = 10.seconds,
  ) extends LlmProfile

  implicit val completionConvert: ConfigConvert[Completion] = deriveConvert[Completion]
  implicit val decisionConvert: ConfigConvert[Decision] = deriveConvert[Decision]

  /** Why `url` cannot name an endpoint, if it cannot, checked by the same `Uri` parser the connection pool uses. */
  private def invalidBaseUrl(url: String): Option[String] =
    Try(Uri(url)).toOption match {
      case None => Some("is not a valid URI")
      case Some(uri) if uri.scheme != "http" && uri.scheme != "https" =>
        Some("must start with http:// or https:// (a bare host:port parses the host as a URI scheme)")
      case Some(uri) if uri.authority.host.isEmpty => Some("has no host")
      case Some(_) => None
    }

  /** The scheduler times at most 2^31 ticks of at least 1 ms, about 24.8 days; past that the pool stream fails. */
  val maxRequestTimeout: FiniteDuration = 24.days

  /** The first out-of-range numeric setting, as its key and message; [[maxRequestTimeout]] caps request-timeout. */
  private def invalidNumericSetting(profile: LlmProfile): Option[(String, String)] = {
    def atLeastOne(key: String, value: Int): Option[(String, String)] =
      Option.when(value <= 0)(key -> s"$key must be at least 1")
    val sizeLimits: Option[(String, String)] = profile match {
      case c: Completion =>
        atLeastOne("max-tokens", c.maxTokens)
          .orElse(c.maxPromptChars.flatMap(atLeastOne("max-prompt-chars", _)))
      case d: Decision => d.maxStateChars.flatMap(atLeastOne("max-state-chars", _))
    }
    atLeastOne("max-concurrent-requests", profile.maxConcurrentRequests)
      .orElse(sizeLimits)
      .orElse(
        Option.when(profile.requestTimeout <= 0.seconds)(
          "request-timeout" -> "request-timeout must be more than zero",
        ),
      )
      .orElse(
        Option.when(profile.requestTimeout > maxRequestTimeout)(
          "request-timeout" -> s"request-timeout must be at most $maxRequestTimeout, the scheduler cannot time more",
        ),
      )
  }

  /** `api.type` picks the variant, so a field of the other variant, or of another api, is an unknown key. The
    * base url and numeric settings are checked here so a mistake fails at startup on its own key path.
    */
  implicit val configConvert: ConfigConvert[LlmProfile] = {
    val reader: ConfigReader[LlmProfile] = ConfigReader.fromCursor { cursor =>
      for {
        obj <- cursor.asObjectCursor
        apiCursor <- obj.atKey("api")
        api <- LlmApi.configConvert.from(apiCursor)
        profile <- api match {
          case _: LlmApi.CompletionApi => completionConvert.from(cursor)
          case _: LlmApi.DecisionApi => decisionConvert.from(cursor)
        }
        _ <- invalidBaseUrl(profile.baseUrl) match {
          case Some(reason) =>
            obj.atKey("base-url").flatMap(_.failed(UserValidationFailed(s"base-url $reason")))
          case None => Right(())
        }
        // Each would otherwise fail later: the pool, the scheduler, a server 400, or a chain that skips the
        // profile on every call. A failing key is always present: defaults are positive, unset limits unchecked
        _ <- invalidNumericSetting(profile) match {
          case Some((key, message)) => obj.atKey(key).flatMap(_.failed(UserValidationFailed(message)))
          case None => Right(())
        }
      } yield profile
    }
    val writer: ConfigWriter[LlmProfile] = ConfigWriter.fromFunction {
      case c: Completion => completionConvert.to(c)
      case d: Decision => decisionConvert.to(d)
    }
    ConfigConvert.fromReaderAndWriter(reader, writer)
  }
}

/** Which profiles each procedure tries, in order, when a call names none. A later profile is tried only
  * when the one before it could not serve the call. In config each default is a list, and a bare string
  * reads as a one-element list. An empty list means no default.
  *
  * @param completion profiles `llm.prompt` tries by default, in order
  * @param decision profiles `llm.decide` tries by default, in order
  */
final case class DefaultProfiles(
  completion: List[String] = Nil,
  decision: List[String] = Nil,
)

/** The `llm` block, under each product's config root. With no profiles the `llm.*` procedures still register and
  * fail on first use.
  */
final case class LlmConfig(
  defaultProfiles: DefaultProfiles = DefaultProfiles(),
  profiles: Map[String, LlmProfile] = Map.empty,
) {

  /** Each profile name must be legal as a metric name component and an HTTP product token, and every name
    * in each default chain must name a configured profile of the matching kind. Every fault is returned,
    * so one load reports everything there is to fix.
    */
  def invalidReasons: List[String] = {
    // The name goes into metric names and the User-Agent; rewriting a bad one could merge two profiles' metrics
    val badNames: List[String] = profiles.keys.toList.sorted.collect {
      case name if !name.matches("[A-Za-z0-9_~-]+") =>
        s"""profile name "$name" may use only letters, digits, '-', '_', and '~', """ +
          "because the name goes into the profile's metric names and User-Agent header"
    }
    // The map holds both variants, so the type cannot say which one a name holds: `matches` asks the
    // class, and `kindName` words the message.
    def check(defaults: List[String], kindName: String)(matches: LlmProfile => Boolean): List[String] =
      defaults.flatMap { name =>
        profiles.get(name) match {
          case None =>
            Some(s"""default-profiles.$kindName names profile "$name", which is not defined under profiles""")
          case Some(profile) if !matches(profile) =>
            Some(s"""default-profiles.$kindName names profile "$name", which has kind ${profile.api.kindName}""")
          case Some(_) => None
        }
      }
    badNames ++
    check(defaultProfiles.completion, LlmApi.completionKindName)(_.isInstanceOf[LlmProfile.Completion]) ++
    check(defaultProfiles.decision, LlmApi.decisionKindName)(_.isInstanceOf[LlmProfile.Decision])
  }
}

object LlmConfig extends PureconfigInstances {
  // The stock instances, summoned before the chain convert exists so they cannot resolve to it
  private val stringListReader: ConfigReader[List[String]] = ConfigReader[List[String]]
  private val stringListWriter: ConfigWriter[List[String]] = ConfigWriter[List[String]]

  implicit val defaultProfilesConvert: ConfigConvert[DefaultProfiles] = {
    // A bare string reads as a one-element chain, so a single default profile needs no list syntax
    implicit val chainConvert: ConfigConvert[List[String]] = ConfigConvert.fromReaderAndWriter(
      stringListReader.orElse(ConfigReader[String].map(List(_))),
      stringListWriter,
    )
    deriveConvert[DefaultProfiles]
  }

  implicit val configConvert: ConfigConvert[LlmConfig] = {
    val derived: ConfigConvert[LlmConfig] = deriveConvert[LlmConfig]
    val validatingReader: ConfigReader[LlmConfig] =
      derived.emap { config =>
        config.invalidReasons match {
          case Nil => Right(config)
          case reasons => Left(UserValidationFailed(reasons.mkString(", and ")))
        }
      }
    ConfigConvert.fromReaderAndWriter(validatingReader, derived)
  }
}
