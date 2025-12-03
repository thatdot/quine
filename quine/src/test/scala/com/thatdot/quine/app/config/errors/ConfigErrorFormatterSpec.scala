package com.thatdot.quine.app.config.errors

import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import pureconfig.error._

class ConfigErrorFormatterSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with ConfigErrorFormatterGen
    with ConfigErrorFormatterHelpers {

  "ConfigErrorFormatter.messageFor" should {

    "handle missing root block correctly across all contexts and configs" in {
      forAll(errorFormatterConfigGen, startupContextGen) { (config, context) =>
        val formatter = new ConfigErrorFormatter(config, context)
        val failure = createKeyNotFoundFailure(config.expectedRootKey, path = "")
        val failures = ConfigReaderFailures(failure)

        val message = formatter.messageFor(failures)

        // Verify key elements are present in message
        message should include(config.expectedRootKey)
        message should include(config.productName)
        message should include(config.docsUrl)

        // Verify context-specific guidance
        context match {
          case StartupContext(Some(file), _) =>
            message should include(file)
          case StartupContext(None, true) =>
            message should include("Running from JAR")
          case StartupContext(None, false) =>
            message should include("application.conf")
        }
      }
    }

    "handle missing required fields with proper kebab-case conversion" in {
      forAll(errorFormatterConfigGen.suchThat(_.requiredFields.nonEmpty), startupContextGen) { (config, context) =>
        val formatter = new ConfigErrorFormatter(config, context)
        val field = config.requiredFields.head // Arbitrary for setup. Position doesn't actually matter.
        val kebabField = field.replaceAll("([a-z])([A-Z])", "$1-$2").toLowerCase
        val failure = createKeyNotFoundFailure(kebabField, path = config.expectedRootKey)
        val failures = ConfigReaderFailures(failure)

        val message = formatter.messageFor(failures)

        message shouldBe
        s"""Configuration error: Missing required '$kebabField'.
           |
           |${config.productName} requires a valid $kebabField to start.
           |
           |Add it to your configuration file:
           |  ${config.expectedRootKey} {
           |    $kebabField = "<your-value>"
           |  }
           |
           |Or set it as a system property:
           |  -D${config.expectedRootKey}.$kebabField=<your-value>
           |
           |For more details, see: ${config.docsUrl}""".stripMargin
      }
    }

    "handle type mismatches with path and type information" in {
      forAll(errorFormatterConfigGen, startupContextGen, nonEmptyAlphaStr, nonEmptyAlphaStr, nonEmptyAlphaStr) {
        (config, context, path, foundType, expectedType) =>

          val formatter = new ConfigErrorFormatter(config, context)
          val failure = createTypeMismatchFailure(path, foundType, expectedType)
          val failures = ConfigReaderFailures(failure)

          val message = formatter.messageFor(failures)

          val pathDisplay = if (path.isEmpty) "root" else s"'$path'"
          val contextGuidance = context.configFile match {
            case Some(file) => s"\nConfiguration file: $file\nSee: ${config.docsUrl}"
            case None => s"\nSee: ${config.docsUrl}"
          }

          message shouldBe
          s"""Configuration error: Invalid type at $pathDisplay.
             |
             |Expected: $expectedType
             |Found: $foundType
             |$contextGuidance""".stripMargin
      }
    }

    "handle unknown configuration keys" in {
      forAll(errorFormatterConfigGen, startupContextGen, nonEmptyAlphaStr, nonEmptyAlphaStr) {
        (config, context, path, unknownKey) =>

          val formatter = new ConfigErrorFormatter(config, context)
          val failure = createUnknownKeyFailure(unknownKey, path)
          val failures = ConfigReaderFailures(failure)

          val message = formatter.messageFor(failures)

          val fullPath = if (path.isEmpty) unknownKey else s"$path.$unknownKey"
          val contextGuidance = context.configFile match {
            case Some(file) => s"\nConfiguration file: $file\nSee: ${config.docsUrl}"
            case None => s"\nSee: ${config.docsUrl}"
          }

          message shouldBe
          s"""Configuration error: Unknown configuration key '$fullPath'.
             |
             |This key is not recognized by ${config.productName}.
             |Check for typos or consult the documentation.
             |$contextGuidance""".stripMargin
      }
    }

    "handle unclassified errors by preserving description" in {
      forAll(errorFormatterConfigGen, startupContextGen, nonEmptyAlphaStr) { (config, context, errorDesc) =>
        val formatter = new ConfigErrorFormatter(config, context)
        val failure = createGenericFailure(errorDesc)
        val failures = ConfigReaderFailures(failure)

        val message = formatter.messageFor(failures)

        val contextGuidance = context.configFile match {
          case Some(file) => s"\nConfiguration file: $file\nSee: ${config.docsUrl}"
          case None => s"\nSee: ${config.docsUrl}"
        }

        val expectedMessage = errorDesc + "\n" + contextGuidance
        message shouldBe expectedMessage
      }
    }

    "format multiple failures as a numbered list" in {
      forAll(errorFormatterConfigGen, startupContextGen, Gen.choose(2, 5)) { (config, context, failureCount) =>
        val formatter = new ConfigErrorFormatter(config, context)

        // Create diverse failure types
        val failures = (1 to failureCount).map { i =>
          i % 3 match {
            case 0 => createKeyNotFoundFailure(s"field-$i", config.expectedRootKey)
            case 1 => createTypeMismatchFailure(s"path$i", "String", "Int")
            case 2 => createUnknownKeyFailure(s"unknown$i", s"path$i")
          }
        }

        val message = formatter.messageFor(ConfigReaderFailures(failures.head, failures.tail: _*))

        // Verify numbered list format
        message should include(s"Found $failureCount configuration errors:")
        (1 to failureCount).foreach { i =>
          message should include(s"$i.")
        }

        // Verify each error type appears
        failures.foreach {
          case ConvertFailure(reason, _, _) =>
            if (reason.description.contains("Key not found")) {
              val key = reason.description.split("'")(1)
              message should include(key)
            } else if (reason.description.contains("Expected type")) {
              message should include("Invalid type")
            } else if (reason.description.contains("Unknown key")) {
              message should include("Unknown configuration key")
            }
          case _ => // Other failure types
        }
      }
    }

    "handle empty path differently from non-empty path in type mismatches" in {
      forAll(errorFormatterConfigGen, startupContextGen) { (config, context) =>
        val formatter = new ConfigErrorFormatter(config, context)
        val foundType = "String"
        val expectedType = "Int"

        // Test with empty path
        val emptyPathFailure = createTypeMismatchFailure("", foundType, expectedType)
        val emptyPathMessage = formatter.messageFor(ConfigReaderFailures(emptyPathFailure))
        val expectedEmptyPath = context.configFile match {
          case Some(file) =>
            s"""Configuration error: Invalid type at root.
               |
               |Expected: $expectedType
               |Found: $foundType
               |
               |Configuration file: $file
               |See: ${config.docsUrl}""".stripMargin
          case None =>
            s"""Configuration error: Invalid type at root.
               |
               |Expected: $expectedType
               |Found: $foundType
               |
               |See: ${config.docsUrl}""".stripMargin
        }
        emptyPathMessage shouldBe expectedEmptyPath

        // Test with non-empty path
        val testPath = "some.path"
        val nonEmptyPathFailure = createTypeMismatchFailure(testPath, foundType, expectedType)
        val nonEmptyPathMessage = formatter.messageFor(ConfigReaderFailures(nonEmptyPathFailure))
        val expectedNonEmptyPath = context.configFile match {
          case Some(file) =>
            s"""Configuration error: Invalid type at '$testPath'.
               |
               |Expected: $expectedType
               |Found: $foundType
               |
               |Configuration file: $file
               |See: ${config.docsUrl}""".stripMargin
          case None =>
            s"""Configuration error: Invalid type at '$testPath'.
               |
               |Expected: $expectedType
               |Found: $foundType
               |
               |See: ${config.docsUrl}""".stripMargin
        }
        nonEmptyPathMessage shouldBe expectedNonEmptyPath
      }
    }

    "consistently convert camelCase field names to kebab-case" in {
      val camelCaseFields = List("maxRetries", "connectionTimeout", "enableSSL")
      val expectedKebab = List("max-retries", "connection-timeout", "enable-ssl")
      val testRootKey = "test-root"
      val testProductName = "Test Product"
      val testDocsUrl = "https://docs.quine.io/"

      forAll(startupContextGen) { context =>
        camelCaseFields.zip(expectedKebab).foreach { case (camelField, kebabField) =>
          val config = ErrorFormatterConfig(
            expectedRootKey = testRootKey,
            productName = testProductName,
            requiredFields = Set(camelField),
            docsUrl = testDocsUrl,
          )

          val formatter = new ConfigErrorFormatter(config, context)
          val failure = createKeyNotFoundFailure(kebabField, testRootKey)
          val message = formatter.messageFor(ConfigReaderFailures(failure))

          message shouldBe
          s"""Configuration error: Missing required '$kebabField'.
               |
               |$testProductName requires a valid $kebabField to start.
               |
               |Add it to your configuration file:
               |  $testRootKey {
               |    $kebabField = "<your-value>"
               |  }
               |
               |Or set it as a system property:
               |  -D$testRootKey.$kebabField=<your-value>
               |
               |For more details, see: $testDocsUrl""".stripMargin
        }
      }
    }

    "include all ErrorFormatterConfig fields in appropriate error messages" in {
      forAll(errorFormatterConfigGen, startupContextGen) { (config, context) =>
        val formatter = new ConfigErrorFormatter(config, context)

        // Test missing root block includes all config fields
        val rootFailure = createKeyNotFoundFailure(config.expectedRootKey, "")
        val rootMessage = formatter.messageFor(ConfigReaderFailures(rootFailure))

        rootMessage should include(config.expectedRootKey)
        rootMessage should include(config.productName)
        rootMessage should include(config.docsUrl)

        // Test missing required field includes relevant config fields
        if (config.requiredFields.nonEmpty) {
          val field = config.requiredFields.head
          val kebabField = field.replaceAll("([a-z])([A-Z])", "$1-$2").toLowerCase
          val fieldFailure = createKeyNotFoundFailure(kebabField, config.expectedRootKey)
          val fieldMessage = formatter.messageFor(ConfigReaderFailures(fieldFailure))

          fieldMessage should include(config.productName)
          fieldMessage should include(config.expectedRootKey)
          fieldMessage should include(config.docsUrl)
        }
      }
    }

    "handle all StartupContext variations appropriately" in {
      forAll(errorFormatterConfigGen, nonEmptyAlphaStr) { (config, confFile) =>
        val formatter1 = new ConfigErrorFormatter(config, StartupContext(None, isJar = false))
        val formatter2 = new ConfigErrorFormatter(config, StartupContext(None, isJar = true))
        val formatter3 = new ConfigErrorFormatter(config, StartupContext(Some(confFile), isJar = false))
        val formatter4 = new ConfigErrorFormatter(config, StartupContext(Some(confFile), isJar = true))

        val failure = createKeyNotFoundFailure(config.expectedRootKey, "")
        val failures = ConfigReaderFailures(failure)

        val message1 = formatter1.messageFor(failures)
        val message2 = formatter2.messageFor(failures)
        val message3 = formatter3.messageFor(failures)
        val message4 = formatter4.messageFor(failures)

        // Verify each context produces different guidance
        message1 should include("application.conf")
        message1 should not include "Running from JAR"

        message2 should include("Running from JAR")
        message2 should include("without a config file")

        message3 should include(confFile)

        message4 should include(confFile)

        // All should include docs URL
        List(message1, message2, message3, message4).foreach(_ should include(config.docsUrl))
      }
    }
  }
}

protected trait ConfigErrorFormatterHelpers {
  private def createConvertFailure(desc: String, path: String): ConvertFailure = {
    val reason = new FailureReason {
      override def description: String = desc
    }
    ConvertFailure(reason, None, path)
  }

  def createKeyNotFoundFailure(key: String, path: String): ConvertFailure =
    createConvertFailure(s"Key not found: '$key'", path)

  def createTypeMismatchFailure(
    path: String,
    found: String,
    expected: String,
  ): ConvertFailure =
    createConvertFailure(s"Expected type $expected. found $found at path", path)

  def createUnknownKeyFailure(key: String, path: String): ConvertFailure =
    createConvertFailure(s"Unknown key '$key'", path)

  def createGenericFailure(description: String): ConfigReaderFailure =
    createConvertFailure(description, path = "")
}

protected trait ConfigErrorFormatterGen {
  val nonEmptyAlphaStr: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString)
  val nonEmptyAlphaStrLower: Gen[String] = nonEmptyAlphaStr.map(_.toLowerCase)
  val nonEmptyAlphaStrNonEmptyList: Gen[List[String]] = Gen.nonEmptyListOf(nonEmptyAlphaStr)

  val kebabCaseStr: Gen[String] = for {
    parts <- nonEmptyAlphaStrNonEmptyList
  } yield parts.mkString("-")

  val camelCaseStr: Gen[String] = for {
    first <- nonEmptyAlphaStrLower
    rest <- Gen.listOf(nonEmptyAlphaStr.map(_.capitalize))
  } yield first + rest.mkString

  val urlGen: Gen[String] = for {
    domain <- nonEmptyAlphaStrLower
    tld <- Gen.oneOf("com", "io", "org", "net")
  } yield s"https://$domain.$tld/"

  val filePathGen: Gen[String] = for {
    segments <- nonEmptyAlphaStrNonEmptyList
    filename <- nonEmptyAlphaStr
  } yield "/" + segments.mkString("/") + "/" + filename + ".conf"

  val startupContextGen: Gen[StartupContext] = for {
    configFile <- Gen.option(filePathGen)
    isJar <- Gen.oneOf(true, false)
  } yield StartupContext(configFile, isJar)

  val errorFormatterConfigGen: Gen[ErrorFormatterConfig] = for {
    expectedRootKey <- kebabCaseStr
    productName <- nonEmptyAlphaStr.map(_.capitalize + " Product")
    requiredFieldCount <- Gen.choose(0, 5)
    requiredFields <- Gen.listOfN(requiredFieldCount, camelCaseStr).map(_.toSet)
    docsUrl <- urlGen
  } yield ErrorFormatterConfig(expectedRootKey, productName, requiredFields, docsUrl)
}
