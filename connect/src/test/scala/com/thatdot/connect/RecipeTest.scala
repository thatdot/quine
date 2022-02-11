package com.thatdot.connect

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8

import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.routes.FileIngestFormat.CypherJson
import com.thatdot.quine.routes.{
  FileIngest,
  IngestStreamConfiguration,
  SampleQuery,
  StandingQueryDefinition,
  StandingQueryPattern,
  StandingQueryResultOutputUserDef,
  UiNodeAppearance,
  UiNodeQuickQuery
}

class RecipeTest extends AnyFunSuite with EitherValues {
  def loadYamlString(s: String): Either[Seq[String], Recipe] =
    Recipe.fromJson(yaml.parseToJson(new ByteArrayInputStream(s.getBytes(UTF_8))))

  def loadRecipeFromClasspath(filename: String): Either[Seq[String], Recipe] =
    Recipe.get(getClass.getResource(filename).toString)

  test("invalid syntax") {
    val expectedParseError =
      """while scanning a simple key
        | in 'reader', line 2, column 1:
        |    baz
        |    ^
        |could not find expected ':'
        | in 'reader', line 3, column 1:
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
      loadYamlString("{}").left.value ==
        List(
          "Missing property 'recipeDefinitionVersionNumber' in JSON object: {}",
          "Missing property 'canonicalName' in JSON object: {}",
          "Missing property 'title' in JSON object: {}",
          "Missing property 'ingestStreams' in JSON object: {}",
          "Missing property 'standingQueries' in JSON object: {}",
          "Missing property 'nodeAppearances' in JSON object: {}",
          "Missing property 'quickQueries' in JSON object: {}",
          "Missing property 'sampleQueries' in JSON object: {}",
          "Missing property 'printQueries' in JSON object: {}"
        )
    )
  }
  test("wrong type") {
    assert(
      loadYamlString("recipeDefinitionVersionNumber: foo").left.value ==
        List(
          """Invalid integer value: "foo"""",
          """Missing property 'canonicalName' in JSON object: {"recipeDefinitionVersionNumber":"foo"}""",
          """Missing property 'title' in JSON object: {"recipeDefinitionVersionNumber":"foo"}""",
          """Missing property 'ingestStreams' in JSON object: {"recipeDefinitionVersionNumber":"foo"}""",
          """Missing property 'standingQueries' in JSON object: {"recipeDefinitionVersionNumber":"foo"}""",
          """Missing property 'nodeAppearances' in JSON object: {"recipeDefinitionVersionNumber":"foo"}""",
          """Missing property 'quickQueries' in JSON object: {"recipeDefinitionVersionNumber":"foo"}""",
          """Missing property 'sampleQueries' in JSON object: {"recipeDefinitionVersionNumber":"foo"}""",
          """Missing property 'printQueries' in JSON object: {"recipeDefinitionVersionNumber":"foo"}"""
        )
    )
  }
  test("minimal recipe") {
    assert(
      loadYamlString("""
          | recipeDefinitionVersionNumber: 1
          | canonicalName: foo
          | title: bar
          | ingestStreams: []
          | standingQueries: []
          | nodeAppearances: []
          | quickQueries: []
          | sampleQueries: []
          | printQueries: []
          |""".stripMargin).value ==
        Recipe(
          recipeDefinitionVersionNumber = 1,
          canonicalName = "foo",
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
          printQueries = List.empty[PrintQuery]
        )
    )
  }
  test("full recipe") {
    assert(
      loadRecipeFromClasspath("/recipes/full.yaml").value ==
        Recipe(
          recipeDefinitionVersionNumber = 1,
          canonicalName = "foo",
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
              pattern = StandingQueryPattern.Cypher(query = "MATCH (n) RETURN id(n)"),
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
          printQueries = List(PrintQuery("match (n) return count(n)"))
        )
    )
  }

  test("string substitution") {
    val values = Map(
      "a" -> "b",
      "c" -> "d",
      "$x" -> "y"
    )
    assert(Recipe.applySubstitution("a", values) == "a")
    assert(Recipe.applySubstitution("$a", values) == "b")
    assert(Recipe.applySubstitution("$c", values) == "d")
    assert(Recipe.applySubstitution("$$a", values) == "$a")
    assert(Recipe.applySubstitution("foo $a bar", values) == "foo $a bar") // internal substitutions not supported
    assertThrows[RuntimeException](Recipe.applySubstitution("$x", values)) // x is not defined
    assert(Recipe.applySubstitution("$$x", values) == "$x") // $$x is not parsed as a token because $$ is a literal $
  }

  test("recipe substitution") {
    val yaml = """
        | recipeDefinitionVersionNumber: 1
        | canonicalName: foo
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
        | printQueries:
        | - cypherQuery: match (n) return count(n)
        |""".stripMargin
    val recipe = loadYamlString(yaml)
    val values = Map(
      "path" -> "/foo/bar"
    )
    assert(
      Recipe.applySubstitutions(recipe.value, values) ==
        Recipe(
          recipeDefinitionVersionNumber = 1,
          canonicalName = "foo",
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
          printQueries = List(PrintQuery("match (n) return count(n)"))
        )
    )
  }
}
