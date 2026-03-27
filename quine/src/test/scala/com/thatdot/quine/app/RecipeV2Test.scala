package com.thatdot.quine.app

import scala.annotation.nowarn
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

import cats.data.{NonEmptyList, Validated}
import io.circe.Error.showError
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, EitherValues}

import com.thatdot.common.logging.Log.{LogConfig, SafeLogger}
import com.thatdot.common.security.Secret
import com.thatdot.quine.app.config.{FileAccessPolicy, ResolutionMode}
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.{IngestFormat, IngestSource}
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern
import com.thatdot.quine.graph.{GraphService, NamespaceId, defaultNamespaceId}
import com.thatdot.quine.serialization.ProtobufSchemaCache

class RecipeV2Test extends AnyFunSuite with EitherValues with BeforeAndAfterAll with Eventually {

  implicit val logConfig: LogConfig = LogConfig.permissive

  val graph: GraphService = IngestTestGraph.makeGraph("recipe-v2-test")

  val quineApp = new QuineApp(
    graph,
    false,
    FileAccessPolicy(List.empty, ResolutionMode.Dynamic),
  )

  val namespace: NamespaceId = defaultNamespaceId

  implicit val ec: ExecutionContext = graph.shardDispatcherEC

  override def beforeAll(): Unit =
    while (!graph.isReady) Thread.sleep(10)

  override def afterAll(): Unit =
    Await.result(graph.shutdown(), 10.seconds)

  private def makeInterpreter(recipe: RecipeV2.Recipe): RecipeInterpreterV2 =
    RecipeInterpreterV2(
      statusLines = new StatusLines(SafeLogger("test-status-lines"), System.err),
      recipe = recipe,
      appState = quineApp,
      graphService = graph,
      quineWebserverUri = None,
      protobufSchemaCache = ProtobufSchemaCache.Blocking: @nowarn("cat=deprecation"),
    )(graph.idProvider)

  def parseV2Yaml(s: String): Either[Seq[String], RecipeV2.Recipe] =
    io.circe.yaml.v12.parser
      .parse(s)
      .left
      .map(e => Seq(showError.show(e)))
      .flatMap(RecipeLoader.parseV2)

  test("recipe v2 parsing test") {
    val yaml = """
        |version: 2
        |title: bar
        |contributor: abc
        |summary: summary
        |description: desc
        |iconImage: http://example.com
        |ingestStreams:
        |  - name: file-ingest
        |    source:
        |      type: File
        |      format:
        |        type: Json
        |      path: /tmp/somefile
        |    query: "CREATE ($that)"
        |standingQueries:
        |  - name: my-sq
        |    pattern:
        |      type: Cypher
        |      query: "MATCH (n) RETURN DISTINCT id(n)"
        |    outputs:
        |      - name: my-output
        |        destinations:
        |          - type: StandardOut
        |nodeAppearances: []
        |quickQueries: []
        |sampleQueries: []
        |statusQuery:
        |  cypherQuery: "MATCH (n) RETURN count(n)"
        |""".stripMargin

    assert(
      parseV2Yaml(yaml).value ==
        RecipeV2.Recipe(
          version = 2,
          title = "bar",
          contributor = Some("abc"),
          summary = Some("summary"),
          description = Some("desc"),
          iconImage = Some("http://example.com"),
          ingestStreams = List(
            RecipeV2.IngestStreamV2(
              name = Some("file-ingest"),
              source = IngestSource.File(
                format = IngestFormat.FileFormat.Json,
                path = "/tmp/somefile",
                fileIngestMode = None,
                limit = None,
              ),
              query = "CREATE ($that)",
            ),
          ),
          standingQueries = List(
            RecipeV2.StandingQueryDefinitionV2(
              name = Some("my-sq"),
              pattern = StandingQueryPattern.Cypher("MATCH (n) RETURN DISTINCT id(n)"),
              outputs = Seq(
                RecipeV2.StandingQueryResultWorkflowV2(
                  name = Some("my-output"),
                  destinations = NonEmptyList.one(QuineDestinationSteps.StandardOut),
                ),
              ),
            ),
          ),
          statusQuery = Some(RecipeV2.StatusQueryV2("MATCH (n) RETURN count(n)")),
        ),
    )
  }

  test("applySubstitution - literal string unchanged") {
    assert(RecipeV2.applySubstitution("hello", Map.empty) == Validated.valid("hello"))
  }

  test("applySubstitution - variable replaced") {
    assert(RecipeV2.applySubstitution("$key", Map("key" -> "value")) == Validated.valid("value"))
  }

  test("applySubstitution - double-dollar escapes to single dollar") {
    assert(RecipeV2.applySubstitution("$$key", Map.empty) == Validated.valid("$key"))
  }

  test("applySubstitution - unbound variable is an error") {
    assert(
      RecipeV2.applySubstitution("$missing", Map.empty) ==
        Validated.invalidNel(RecipeV2.UnboundVariableError("missing")),
    )
  }

  test("applySubstitutions for file path") {
    val yaml = """
        |version: 2
        |title: test
        |ingestStreams:
        |  - source:
        |      type: File
        |      format:
        |        type: Json
        |      path: $dataPath
        |    query: "CREATE ($that)"
        |""".stripMargin
    val recipe = parseV2Yaml(yaml).value
    val result = RecipeV2.applySubstitutions(recipe, Map("dataPath" -> "/data/file.json"))
    assert(result.isValid)
    result.toOption.get.ingestStreams.head.source match {
      case f: IngestSource.File => assert(f.path == "/data/file.json")
      case other => fail(s"Expected File ingest source, got $other")
    }
  }

  test("applySubstitutions for AWS credentials in SQS ingest") {
    import Secret.Unsafe._
    val yaml = """
        |version: 2
        |title: sqs-test
        |ingestStreams:
        |  - source:
        |      type: SQS
        |      format:
        |        type: Json
        |      queueUrl: $queueUrl
        |      credentials:
        |        accessKeyId: $accessKey
        |        secretAccessKey: $secretKey
        |    query: "CREATE ($that)"
        |""".stripMargin
    val recipe = parseV2Yaml(yaml).value
    val result = RecipeV2.applySubstitutions(
      recipe,
      Map(
        "queueUrl" -> "https://sqs.us-east-1.amazonaws.com/123/my-queue",
        "accessKey" -> "AKIAIOSFODNN7EXAMPLE",
        "secretKey" -> "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      ),
    )
    assert(result.isValid, s"Substitution failed: $result")
    result.toOption.get.ingestStreams.head.source match {
      case sqs: IngestSource.SQS =>
        assert(sqs.queueUrl == "https://sqs.us-east-1.amazonaws.com/123/my-queue")
        val creds = sqs.credentials.get
        assert(creds.accessKeyId.unsafeValue == "AKIAIOSFODNN7EXAMPLE")
        assert(creds.secretAccessKey.unsafeValue == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
      case other => fail(s"Expected SQS ingest source, got $other")
    }
  }

  test("applySubstitutions error accumulation") {
    val yaml = """
        |version: 2
        |title: test
        |ingestStreams:
        |  - source:
        |      type: File
        |      format:
        |        type: Json
        |      path: $path1
        |    query: "$query1"
        |  - source:
        |      type: File
        |      format:
        |        type: Json
        |      path: $path2
        |    query: "CREATE ($that)"
        |""".stripMargin
    val recipe = parseV2Yaml(yaml).value
    val result = RecipeV2.applySubstitutions(recipe, Map.empty)
    assert(result.isInvalid)
    val errors = result.fold(_.toList, _ => Nil)
    assert(errors.contains(RecipeV2.UnboundVariableError("path1")))
    assert(errors.contains(RecipeV2.UnboundVariableError("query1")))
    assert(errors.contains(RecipeV2.UnboundVariableError("path2")))
  }

  test("applySubstitutions for SQ destination URL") {
    val yaml = """
        |version: 2
        |title: test
        |standingQueries:
        |  - pattern:
        |      type: Cypher
        |      query: "MATCH (n) RETURN DISTINCT id(n)"
        |    outputs:
        |      - destinations:
        |          - type: HttpEndpoint
        |            url: $endpointUrl
        |""".stripMargin
    val recipe = parseV2Yaml(yaml).value
    val result = RecipeV2.applySubstitutions(recipe, Map("endpointUrl" -> "https://example.com/hook"))
    assert(result.isValid)
    result.toOption.get.standingQueries.head.outputs.head.destinations.head match {
      case h: QuineDestinationSteps.HttpEndpoint => assert(h.url == "https://example.com/hook")
      case other => fail(s"Expected HttpEndpoint destination, got $other")
    }
  }

  test("v2 recipe interpreter properly registers a standing query in the recipe") {
    val sqName = "sq-registration-sq"
    val recipe = RecipeV2.Recipe(
      title = "sq-registration-test",
      standingQueries = List(
        RecipeV2.StandingQueryDefinitionV2(
          name = Some(sqName),
          pattern = StandingQueryPattern.Cypher("MATCH (n) WHERE n.name IS NOT NULL RETURN DISTINCT id(n)"),
          outputs = Seq(
            RecipeV2.StandingQueryResultWorkflowV2(
              name = Some("stdout"),
              destinations = NonEmptyList.one(QuineDestinationSteps.StandardOut),
            ),
          ),
        ),
      ),
    )

    val interpreter = makeInterpreter(recipe)
    try {
      interpreter.run(memberIdx = 0)
      val sq = Await.result(quineApp.getStandingQueryV2(sqName, namespace), 5.seconds)
      assert(sq.isDefined)
      assert(sq.get.name == sqName)
    } finally { val _ = interpreter.cancel() }
  }

  test("v2 interpreter properly registers ingest streams from recipe") {
    val ingestName = "ingest-registration-ingest"
    val recipe = RecipeV2.Recipe(
      title = "ingest-registration-test",
      ingestStreams = List(
        RecipeV2.IngestStreamV2(
          name = Some(ingestName),
          source = IngestSource.NumberIterator(limit = Some(0L)),
          query = "MATCH (n) WHERE id(n) = idFrom($that) SET n.visited = true",
        ),
      ),
    )

    val interpreter = makeInterpreter(recipe)
    try {
      interpreter.run(memberIdx = 0)
      assert(quineApp.getIngestStream(ingestName, namespace).isDefined)
    } finally { val _ = interpreter.cancel() }
  }

  test("interpreter ingests records into the graph") {
    val ingestName = "ingest-data-ingest"
    val recipe = RecipeV2.Recipe(
      title = "ingest-data-test",
      ingestStreams = List(
        RecipeV2.IngestStreamV2(
          name = Some(ingestName),
          source = IngestSource.NumberIterator(limit = Some(2L)),
          query = "MATCH (n) WHERE id(n) = idFrom($that) SET n.number = $that",
        ),
      ),
    )

    val interpreter = makeInterpreter(recipe)
    try {
      interpreter.run(memberIdx = 0)
      eventually(Eventually.timeout(10.seconds), interval(500.millis)) {
        val ingestedCount = quineApp
          .getIngestStream(ingestName, namespace)
          .map(_.metrics.toEndpointResponse.ingestedCount)
          .getOrElse(0L)
        assert(ingestedCount == 2L)
      }
    } finally { val _ = interpreter.cancel() }
  }
}
