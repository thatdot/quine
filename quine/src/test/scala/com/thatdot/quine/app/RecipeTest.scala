package com.thatdot.quine.app

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8

import cats.data.{NonEmptyList, Validated}
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.routes.FileIngestFormat.CypherJson
import com.thatdot.quine.routes._

class RecipeTest extends AnyFunSuite with EitherValues {
  def loadYamlString(s: String): Either[Seq[String], Recipe] =
    Recipe.fromJson(yaml.parseToJson(new ByteArrayInputStream(s.getBytes(UTF_8))))

  def loadRecipeFromClasspath(filename: String): Either[Seq[String], Recipe] =
    Recipe.get(getClass.getResource(filename).toString)

  test("invalid syntax") {
    val expectedParseError =
      """while scanning a simple key
        | in reader, line 2, column 1:
        |    baz
        |    ^
        |could not find expected ':'
        | in reader, line 3, column 1:
        |    blah
        |    ^
        |""".stripMargin
    assert(
      loadRecipeFromClasspath("/yaml/invalid.yaml").left.value == Seq(expectedParseError)
    )
  }

  test("not an object") {
    assert(
      loadYamlString("foo").left.value == Seq("""Recipe must be an object, got: "foo"""")
    )
  }
  test("empty object") {
    assert(
      loadYamlString("{}") == Right(
        Recipe(Recipe.currentVersion, "RECIPE", None, None, None, None, List(), List(), List(), List(), List(), None)
      )
    )
  }

  test("invalid keys") {

    val recipe = loadYamlString("""
          |version: 11
          |not_a_key: 2
          |also_not_a_key: 3
      """.stripMargin)

    assert(
      recipe.left.value.toSet == Set(
        "The field 'not_a_key' is not part of the recipe specification",
        "The field 'also_not_a_key' is not part of the recipe specification"
      )
    )
  }

  test("validation") {

    val recipe = loadYamlString("""
        |version: 2
      """.stripMargin)

    assert(recipe.left.value == Seq("The only supported Recipe version number is 1"))

  }
  test("wrong type") {
    assert(
      loadYamlString("version: foo").left.value ==
        List(
          "DecodingFailure at .version: Int"
        )
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
        Recipe(
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
          statusQuery = None
        )
    )
  }
  test("full recipe") {
    assert(
      loadRecipeFromClasspath("/recipes/full.yaml").value ==
        Recipe(
          version = 1,
          title = "bar",
          contributor = Some("abc"),
          summary = Some("summary"),
          description = Some("desc"),
          iconImage = Some("http://example.com"),
          ingestStreams = List(
            FileIngest(
              format = CypherJson(
                query = "yadda"
              ),
              path = "/tmp/somefile",
              ingestLimit = None,
              maximumPerSecond = None,
              fileIngestMode = None
            )
          ),
          standingQueries = List(
            StandingQueryDefinition(
              pattern = StandingQueryPattern.Cypher(query = "MATCH (n) RETURN DISTINCT id(n)"),
              outputs = Map(
                "output-1" -> StandingQueryResultOutputUserDef.CypherQuery(
                  query = "X",
                  parameter = "bar",
                  andThen = None
                )
              )
            )
          ),
          nodeAppearances = List.empty[UiNodeAppearance],
          quickQueries = List.empty[UiNodeQuickQuery],
          sampleQueries = List.empty[SampleQuery],
          statusQuery = Some(StatusQuery("MATCH (n) RETURN count(n)"))
        )
    )
  }

  test("string substitution") {
    val values = Map(
      "a" -> "b",
      "c" -> "d",
      "$x" -> "y"
    )
    assert(Recipe.applySubstitution("a", values) == Validated.valid("a"))
    assert(Recipe.applySubstitution("$a", values) == Validated.valid("b"))
    assert(Recipe.applySubstitution("$c", values) == Validated.valid("d"))
    assert(Recipe.applySubstitution("$$a", values) == Validated.valid("$a"))

    // internal substitutions not supported
    assert(Recipe.applySubstitution("foo $a bar", values) == Validated.valid("foo $a bar"))

    // x is not defined
    assert(Recipe.applySubstitution("$x", values) == Validated.invalid(Recipe.UnboundVariableError("x")).toValidatedNel)

    // $$x is not parsed as a token because $$ is a literal $
    assert(Recipe.applySubstitution("$$x", values) == Validated.valid("$x"))
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
      "path" -> "/foo/bar"
    )
    assert(
      Recipe.applySubstitutions(recipe.value, values) == Validated.valid(
        Recipe(
          version = 1,
          title = "bar",
          contributor = Some("abc"),
          summary = Some("summary"),
          description = Some("desc"),
          iconImage = Some("http://example.com"),
          ingestStreams = List(
            FileIngest(
              format = CypherJson(
                query = "yadda"
              ),
              path = "/foo/bar",
              ingestLimit = None,
              maximumPerSecond = None,
              fileIngestMode = None
            )
          ),
          standingQueries = List.empty[StandingQueryDefinition],
          nodeAppearances = List.empty[UiNodeAppearance],
          quickQueries = List.empty[UiNodeQuickQuery],
          sampleQueries = List.empty[SampleQuery],
          statusQuery = Some(StatusQuery("match (n) return count(n)"))
        )
      )
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
      "path2" -> "/foo/bar"
    )
    assert(
      Recipe.applySubstitutions(recipe.value, values) == Validated.invalid(
        NonEmptyList.of(
          Recipe.UnboundVariableError("path1"),
          Recipe.UnboundVariableError("path4"),
          Recipe.UnboundVariableError("path3")
        )
      )
    )
  }

}
