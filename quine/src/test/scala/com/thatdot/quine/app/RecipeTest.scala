package com.thatdot.quine.app

import cats.data.{NonEmptyList, Validated}
import cats.syntax.either._
import io.circe.CursorOp.DownField
import io.circe.DecodingFailure.Reason.{CustomReason, WrongTypeExpectation}
import io.circe.{DecodingFailure, Json}
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.routes.FileIngestFormat.CypherJson
import com.thatdot.quine.routes._

class RecipeTest extends AnyFunSuite with EitherValues {
  def loadYamlString(s: String): Either[NonEmptyList[io.circe.Error], RecipeV1] =
    io.circe.yaml.v12.parser.parse(s).toEitherNel.flatMap(RecipeV1.fromJson)

  def loadRecipeFromClasspath(filename: String): Either[Seq[String], RecipeV1] =
    RecipeV1.get(getClass.getResource(filename).toString)

  test("invalid syntax") {
    val expectedParseError =
      """ParsingFailure: while scanning a simple key
        | in reader, line 2, column 1:
        |    baz
        |    ^
        |could not find expected ':'
        | in reader, line 3, column 1:
        |    blah
        |    ^
        |""".stripMargin
    assert(
      loadRecipeFromClasspath("/yaml/invalid.yaml").left.value == Seq(expectedParseError),
    )
  }

  test("not an object") {
    assert(
      loadYamlString("foo").left.value == NonEmptyList.one(
        DecodingFailure(WrongTypeExpectation("object", Json.fromString("foo")), List()),
      ),
    )
  }
  test("empty object") {
    assert(
      loadYamlString("{}") == Right(
        RecipeV1(
          RecipeV1.currentVersion,
          "RECIPE",
          None,
          None,
          None,
          None,
          List(),
          List(),
          List(),
          List(),
          List(),
          None,
        ),
      ),
    )
  }

  test("invalid keys") {

    val recipe = loadYamlString("""
          |version: 11
          |not_a_key: 2
          |also_not_a_key: 3
      """.stripMargin)

    assert(
      recipe.left.value == NonEmptyList.of(
        DecodingFailure(
          "Unexpected field: [not_a_key]; valid fields: version, title, contributor, summary, description, iconImage, ingestStreams, standingQueries, nodeAppearances, quickQueries, sampleQueries, statusQuery",
          List(),
        ),
        DecodingFailure(
          "Unexpected field: [also_not_a_key]; valid fields: version, title, contributor, summary, description, iconImage, ingestStreams, standingQueries, nodeAppearances, quickQueries, sampleQueries, statusQuery",
          List(),
        ),
      ),
    )
  }

  test("validation") {

    val recipe = loadYamlString("""
        |version: 2
      """.stripMargin).value

    assert(
      RecipeV1.validateRecipeCurrentVersion(recipe).left.value == Seq(
        "Recipe version 2 is not supported by this method. Use Recipe.get() for V2 recipes.",
      ),
    )

  }
  test("wrong types") {
    loadYamlString("version: foo\ntitle: 6").left.value ==
      NonEmptyList.of(
        DecodingFailure(CustomReason("Int"), List(DownField("version"))),
        DecodingFailure(WrongTypeExpectation("string", Json.fromInt(6)), List(DownField("title"))),
      )

  }
  test("minimal recipe") {
    assert(
      loadYamlString("""
          | version: 1
          | title: bar
          | ingestStreams: []
          | standingQueries: []
          | nodeAppearances: []
          | quickQueries: []
          | sampleQueries: []
          | statusQuery: null # need to verify this works
          |""".stripMargin).value ==
        RecipeV1(
          version = 1,
          title = "bar",
          contributor = None,
          summary = None,
          description = None,
          iconImage = None,
          ingestStreams = List.empty[IngestStreamConfiguration],
          standingQueries = List.empty[StandingQueryDefinition],
          nodeAppearances = List.empty[UiNodeAppearance],
          quickQueries = List.empty[UiNodeQuickQuery],
          sampleQueries = List.empty[SampleQuery],
          statusQuery = None,
        ),
    )
  }
  test("full recipe") {
    assert(
      loadRecipeFromClasspath("/recipes/full.yaml").value ==
        RecipeV1(
          version = 1,
          title = "bar",
          contributor = Some("abc"),
          summary = Some("summary"),
          description = Some("desc"),
          iconImage = Some("http://example.com"),
          ingestStreams = List(
            FileIngest(
              format = CypherJson(
                query = "yadda",
              ),
              path = "/tmp/somefile",
              ingestLimit = None,
              maximumPerSecond = None,
              fileIngestMode = None,
            ),
          ),
          standingQueries = List(
            StandingQueryDefinition(
              pattern = StandingQueryPattern.Cypher(query = "MATCH (n) RETURN DISTINCT id(n)"),
              outputs = Map(
                "output-1" -> StandingQueryResultOutputUserDef.CypherQuery(
                  query = "X",
                  parameter = "bar",
                  andThen = None,
                ),
              ),
            ),
          ),
          nodeAppearances = List.empty[UiNodeAppearance],
          quickQueries = List.empty[UiNodeQuickQuery],
          sampleQueries = List.empty[SampleQuery],
          statusQuery = Some(StatusQuery("MATCH (n) RETURN count(n)")),
        ),
    )
  }

  test("string substitution") {
    val values = Map(
      "a" -> "b",
      "c" -> "d",
      "$x" -> "y",
    )
    assert(RecipeV1.applySubstitution("a", values) == Validated.valid("a"))
    assert(RecipeV1.applySubstitution("$a", values) == Validated.valid("b"))
    assert(RecipeV1.applySubstitution("$c", values) == Validated.valid("d"))
    assert(RecipeV1.applySubstitution("$$a", values) == Validated.valid("$a"))

    // internal substitutions not supported
    assert(RecipeV1.applySubstitution("foo $a bar", values) == Validated.valid("foo $a bar"))

    // x is not defined
    assert(
      RecipeV1.applySubstitution("$x", values) == Validated.invalid(RecipeV1.UnboundVariableError("x")).toValidatedNel,
    )

    // $$x is not parsed as a token because $$ is a literal $
    assert(RecipeV1.applySubstitution("$$x", values) == Validated.valid("$x"))
  }

  test("recipe substitution") {
    val yaml = """
        | version: 1
        | title: bar
        | contributor: abc
        | summary: summary
        | description: desc
        | iconImage: http://example.com
        | ingestStreams:
        | - type: FileIngest
        |   path: $path
        |   format:
        |     type: CypherJson
        |     query: yadda
        | standingQueries: []
        | nodeAppearances: []
        | quickQueries: []
        | sampleQueries: []
        | statusQuery:
        |   cypherQuery: match (n) return count(n)
        |""".stripMargin
    val recipe = loadYamlString(yaml)
    val values = Map(
      "path" -> "/foo/bar",
    )
    assert(
      RecipeV1.applySubstitutions(recipe.value, values) == Validated.valid(
        RecipeV1(
          version = 1,
          title = "bar",
          contributor = Some("abc"),
          summary = Some("summary"),
          description = Some("desc"),
          iconImage = Some("http://example.com"),
          ingestStreams = List(
            FileIngest(
              format = CypherJson(
                query = "yadda",
              ),
              path = "/foo/bar",
              ingestLimit = None,
              maximumPerSecond = None,
              fileIngestMode = None,
            ),
          ),
          standingQueries = List.empty[StandingQueryDefinition],
          nodeAppearances = List.empty[UiNodeAppearance],
          quickQueries = List.empty[UiNodeQuickQuery],
          sampleQueries = List.empty[SampleQuery],
          statusQuery = Some(StatusQuery("match (n) return count(n)")),
        ),
      ),
    )
  }

  test("recipe substitution errors") {
    val yaml = """
        | version: 1
        | title: bar
        | contributor: abc
        | summary: summary
        | description: desc
        | iconImage: http://example.com
        | ingestStreams:
        | - type: FileIngest
        |   path: $path1
        |   format:
        |     type: CypherJson
        |     query: yadda
        | - type: FileIngest
        |   path: $path2
        |   format:
        |     type: CypherJson
        |     query: yadda
        | - type: FileIngest
        |   path: $path4
        |   format:
        |     type: CypherJson
        |     query: yadda
        | - type: FileIngest
        |   path: $path3
        |   format:
        |     type: CypherJson
        |     query: yadda
        | standingQueries: []
        | nodeAppearances: []
        | quickQueries: []
        | sampleQueries: []
        | statusQuery:
        |   cypherQuery: match (n) return count(n)
        |""".stripMargin
    val recipe = loadYamlString(yaml)
    val values = Map(
      "path2" -> "/foo/bar",
    )
    assert(
      RecipeV1.applySubstitutions(recipe.value, values) == Validated.invalid(
        NonEmptyList.of(
          RecipeV1.UnboundVariableError("path1"),
          RecipeV1.UnboundVariableError("path4"),
          RecipeV1.UnboundVariableError("path3"),
        ),
      ),
    )
  }

  test("recipe substitution for AWS credentials") {
    import com.thatdot.common.security.Secret
    import Secret.Unsafe._

    val yaml = """
        | version: 1
        | title: sqs-test
        | ingestStreams:
        | - type: SQSIngest
        |   queueUrl: https://sqs.us-east-1.amazonaws.com/123456789/my-queue
        |   credentials:
        |     accessKeyId: $accessKey
        |     secretAccessKey: $secretKey
        |   format:
        |     type: CypherJson
        |     query: CREATE ($that)
        | standingQueries: []
        | nodeAppearances: []
        | quickQueries: []
        | sampleQueries: []
        |""".stripMargin
    val recipe = loadYamlString(yaml)
    val values = Map(
      "accessKey" -> "AKIAIOSFODNN7EXAMPLE",
      "secretKey" -> "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )

    val substituted = RecipeV1.applySubstitutions(recipe.value, values)
    assert(substituted.isValid, s"Substitution failed: $substituted")

    val resultRecipe = substituted.toOption.get
    val sqsIngest = resultRecipe.ingestStreams.head.asInstanceOf[SQSIngest]
    val creds = sqsIngest.credentials.get

    assert(creds.accessKeyId.unsafeValue == "AKIAIOSFODNN7EXAMPLE")
    assert(creds.secretAccessKey.unsafeValue == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
  }

  test("recipe canonical name") {
    val invalidShortName: String = "somethingElse"
    val validShortName: String = "wikipedia"
    val url: String = "https://raw.githubusercontent.com/thatdot/quine/main/quine/recipes/ethereum.yaml"
    val fileName: String = "wikipedia.yaml"

    // Currently, the getCanonicalName function does not distinguish between a "valid" or "invalid" canonical name.
    // For telemetry, the value will only be sent if the recipe was successfully loaded, so only "valid" recipe names
    // actually appear in telemetry. Therefore, this "invalid" name should still return a Some().
    assert(RecipeV1.getCanonicalName(invalidShortName).contains(invalidShortName))
    // Valid canonical name should return a Some()
    assert(RecipeV1.getCanonicalName(validShortName).contains(validShortName))
    // any url should return None
    assert(RecipeV1.getCanonicalName(url).isEmpty)
    // any file name should return None
    assert(RecipeV1.getCanonicalName(fileName).isEmpty)
  }

}
