package com.thatdot.quine.app.config.errors

import cats.data.NonEmptyList
import pureconfig.error.{ConfigReaderFailure, ConfigReaderFailures, ConvertFailure, FailureReason}

/** Configuration for error message formatting */
final case class ErrorFormatterConfig(
  expectedRootKey: String,
  productName: String,
  requiredFields: Set[String],
  docsUrl: String,
)

object ErrorFormatterConfig {

  /** Format configuration errors with automatically detected startup context */
  def formatErrors(config: ErrorFormatterConfig, failures: ConfigReaderFailures): String = {
    val context = StartupContext(
      configFile = sys.props.get("config.file"),
      isJar = !sys.props.get("java.class.path").exists(_.contains("sbt-launch")),
    )
    new ConfigErrorFormatter(config, context).messageFor(failures)
  }
}

sealed trait ConfigError {
  def format(config: ErrorFormatterConfig, context: StartupContext): String
}

object ConfigError {
  case object MissingRootBlock extends ConfigError {
    override def format(config: ErrorFormatterConfig, context: StartupContext): String = {
      val basicMessage =
        s"""Configuration error: Missing '${config.expectedRootKey}' configuration block.
           |
           |${config.productName} requires all configuration to be nested under a '${config.expectedRootKey}' block.""".stripMargin

      val guidance = context match {
        case StartupContext(Some(file), _) =>
          s"""
             |Configuration file: $file
             |
             |Ensure it has the correct structure:
             |  ${config.expectedRootKey} {
             |    # your configuration here
             |  }""".stripMargin

        case StartupContext(None, true) =>
          s"""
             |Running from JAR without a config file.
             |
             |You must either:
             |  1. Provide a config file: -Dconfig.file=<path-to-config.conf>
             |  2. Set required properties: -D${config.expectedRootKey}.license-key=<your-key>""".stripMargin

        case StartupContext(None, false) =>
          s"""
             |Provide configuration via:
             |  1. application.conf in your classpath
             |  2. System properties: -D${config.expectedRootKey}.license-key=<your-key>
             |  3. Config file: -Dconfig.file=<path-to-config.conf>""".stripMargin
      }

      basicMessage + guidance + s"\n\nFor more details, see: ${config.docsUrl}"
    }
  }

  final case class MissingRequiredField(fieldName: String) extends ConfigError {
    override def format(config: ErrorFormatterConfig, context: StartupContext): String = {
      val kebabFieldName = toKebabCase(fieldName)

      s"""Configuration error: Missing required '$kebabFieldName'.
         |
         |${config.productName} requires a valid $kebabFieldName to start.
         |
         |Add it to your configuration file:
         |  ${config.expectedRootKey} {
         |    $kebabFieldName = "<your-value>"
         |  }
         |
         |Or set it as a system property:
         |  -D${config.expectedRootKey}.$kebabFieldName=<your-value>
         |
         |For more details, see: ${config.docsUrl}""".stripMargin
    }
  }

  final case class Invalid(path: String, found: String, expected: Set[String]) extends ConfigError {
    override def format(config: ErrorFormatterConfig, context: StartupContext): String = {
      val pathDisplay = if (path.isEmpty) "root" else s"'$path'"
      val expectedDisplay = if (expected.size == 1) expected.head else expected.mkString(" or ")

      s"""Configuration error: Invalid type at $pathDisplay.
         |
         |Expected: $expectedDisplay
         |Found: $found
         |${contextGuidance(context, config)}""".stripMargin
    }
  }

  final case class UnknownConfigKey(path: String, key: String) extends ConfigError {
    override def format(config: ErrorFormatterConfig, context: StartupContext): String = {
      val fullPath = if (path.isEmpty) key else s"$path.$key"

      s"""Configuration error: Unknown configuration key '$fullPath'.
         |
         |This key is not recognized by ${config.productName}.
         |Check for typos or consult the documentation.
         |${contextGuidance(context, config)}""".stripMargin
    }
  }

  /** Unclassified error - we couldn't parse/classify this failure.
    * Retains original failure for debugging.
    */
  final case class UnclassifiedError(
    description: String,
    originalFailure: Option[ConfigReaderFailure] = None,
  ) extends ConfigError {
    override def format(config: ErrorFormatterConfig, context: StartupContext): String =
      description + "\n" + contextGuidance(context, config)
  }

  private def contextGuidance(
    context: StartupContext,
    config: ErrorFormatterConfig,
  ): String = context.configFile match {
    case Some(file) => s"\nConfiguration file: $file\nSee: ${config.docsUrl}"
    case None => s"\nSee: ${config.docsUrl}"
  }

  private[errors] def toKebabCase(camelCase: String): String =
    camelCase.replaceAll("([a-z])([A-Z])", "$1-$2").toLowerCase
}

/** Context about how the app was started */
final case class StartupContext(
  configFile: Option[String],
  isJar: Boolean,
)

/** Formats config errors with context. */
class ConfigErrorFormatter(
  config: ErrorFormatterConfig,
  context: StartupContext,
) {

  /** Generate user-friendly error message for all configuration failures.
    * Processes each failure individually and combines them intelligently.
    */
  def messageFor(failures: ConfigReaderFailures): String = {
    // ConfigReaderFailures is guaranteed non-empty by PureConfig
    val errorTypes = NonEmptyList.fromListUnsafe(failures.toList.map(classifyFailure))
    combineMessages(errorTypes)
  }

  private def combineMessages(errorTypes: NonEmptyList[ConfigError]): String =
    errorTypes match {
      case NonEmptyList(single, Nil) =>
        single.format(config, context)

      case multiple =>
        val header = s"Found ${multiple.size} configuration errors:\n"
        val formattedErrors = multiple.toList.zipWithIndex.map { case (error, idx) =>
          s"${idx + 1}. ${error.format(config, context)}"
        }
        header + formattedErrors.mkString("\n\n")
    }

  private def classifyFailure(failure: ConfigReaderFailure): ConfigError =
    failure match {
      case ConvertFailure(reason, _, path) if path.isEmpty && isKeyNotFound(reason, config.expectedRootKey) =>
        ConfigError.MissingRootBlock

      case ConvertFailure(reason, _, path) =>
        config.requiredFields
          .collectFirst {
            case fieldName if isKeyNotFound(reason, ConfigError.toKebabCase(fieldName)) =>
              ConfigError.MissingRequiredField(fieldName)
          }
          .getOrElse(ConfigErrorFormatter.classifyUnknownFailure(reason, path))

      case other =>
        ConfigError.UnclassifiedError(other.description, Some(other))
    }

  private def isKeyNotFound(reason: FailureReason, expectedKey: String): Boolean = {
    val desc = reason.description
    // Match exact key or parent keys (e.g., "thatdot" when expecting "thatdot.novelty")
    desc.contains(s"Key not found: '$expectedKey'") ||
    expectedKey.split('.').exists(part => desc.contains(s"Key not found: '$part'"))
  }
}

object ConfigErrorFormatter {

  /** Classify failures that don't match known patterns.
    * Parses the failure reason description and maps to appropriate ConfigError types.
    */
  private def classifyUnknownFailure(reason: FailureReason, path: String): ConfigError = {
    val desc = reason.description

    if (desc.contains("Expected type") || desc.contains("Wrong type")) {
      val result = for {
        found <- extractBetween(desc, "Found ", " ").orElse(extractBetween(desc, "found ", " "))
        expected <- extractBetween(desc, "Expected type ", ".")
      } yield ConfigError.Invalid(path, found, Set(expected))

      result.getOrElse(ConfigError.UnclassifiedError(desc, None))
    } else if (desc.contains("Unknown key")) {
      extractBetween(desc, "Unknown key '", "'")
        .map(key => ConfigError.UnknownConfigKey(path, key))
        .getOrElse(ConfigError.UnclassifiedError(desc, None))
    } else {
      ConfigError.UnclassifiedError(desc, None)
    }
  }

  /** Extract text between two markers (helper for parsing descriptions)
    * Returns None if start marker not found, Some(text) otherwise.
    */
  private def extractBetween(text: String, start: String, end: String): Option[String] =
    for {
      startIdx <- Option.when(text.contains(start))(text.indexOf(start))
      afterStart = text.substring(startIdx + start.length)
      endIdx = afterStart.indexOf(end)
      result = if (endIdx >= 0) afterStart.substring(0, endIdx) else afterStart
    } yield result
}
