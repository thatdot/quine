package com.thatdot.connect

import scala.util.Using

import endpoints4s.generic.docs

import com.thatdot.connect.yaml.parseToJson
import com.thatdot.quine.routes._
import com.thatdot.quine.routes.exts.UjsonAnySchema
import com.thatdot.quine.util.StringInput.filenameOrUrl

import StandingQueryResultOutputUserDef._

@docs("A specification of a Quine Recipe")
final case class Recipe(
  @docs("Schema version (only supported value is 1)") recipeDefinitionVersionNumber: Int,
  @docs("Globally unique identifier for this Recipe") canonicalName: String,
  @docs("Identifies the Recipe but is not necessarily unique") title: String,
  @docs("Email address of the person or organization responsible for this Recipe") contributor: Option[String],
  @docs("Brief copy about this Recipe") summary: Option[String],
  @docs("Longer form copy about this Recipe") description: Option[String],
  @docs("URL to image asset for this Recipe") iconImage: Option[String],
  @docs("Ingest Streams that load data into the graph") ingestStreams: List[IngestStreamConfiguration],
  @docs(
    "Standing Queries that respond to graph updates by computing aggregates and trigger actions"
  ) standingQueries: List[StandingQueryDefinition],
  @docs("For web UI customization") nodeAppearances: List[UiNodeAppearance],
  @docs("For web UI customization") quickQueries: List[UiNodeQuickQuery],
  @docs("For web UI customization") sampleQueries: List[SampleQuery],
  @docs("Cypher queries that are run periodically while Recipe is running") printQueries: List[PrintQuery]
)

@docs("A Cypher query to be run periodically while Recipe is running")
final case class PrintQuery(cypherQuery: String)

private object RecipeSchema
    extends endpoints4s.ujson.JsonSchemas
    with endpoints4s.generic.JsonSchemas
    with IngestSchemas
    with StandingQuerySchemas
    with QueryUiConfigurationSchemas
    with UjsonAnySchema {

  implicit lazy val printQuerySchema: JsonSchema[PrintQuery] =
    genericJsonSchema[PrintQuery]

  implicit lazy val recipeSchema: JsonSchema[Recipe] =
    genericJsonSchema[Recipe]
}

object Recipe {

  import RecipeSchema._

  def fromJson(json: ujson.Value): Either[Seq[String], Recipe] = for {
    jsonObj <- json.objOpt.map(ujson.Obj(_)) toRight Seq("Recipe must be an object, got: " + json)
    recipe <- recipeSchema.decoder.decode(jsonObj).toEither
  } yield recipe

  /** Produces a copy of the Recipe with all tokens substituted with defined values. Only certain
    * predetermined Recipe fields are processed in this way. If a token is undefined, it fails with an
    * exception.
    */
  def applySubstitutions(recipe: Recipe, values: Map[String, String]): Recipe = {
    // Implicit classes so that .subs can be used below.
    implicit class Subs(s: String) {
      def subs: String = applySubstitution(s, values)
    }
    implicit class SubCreds(c: AwsCredentials) {
      def subs: AwsCredentials = AwsCredentials(
        c.region.subs,
        c.accessKeyId.subs,
        c.secretAccessKey.subs
      )
    }
    implicit class SubStandingQueryOutputSubs(soo: StandingQueryResultOutputUserDef) {
      def subs: StandingQueryResultOutputUserDef = soo match {
        case PostToEndpoint(url, parallelism, onlyPositiveMatchData) =>
          PostToEndpoint(url.subs, parallelism, onlyPositiveMatchData)
        case WriteToKafka(topic, bootstrapServers, format) =>
          WriteToKafka(topic.subs, bootstrapServers.subs, format)
        case WriteToSNS(credentials, topic) => WriteToSNS(credentials.map(_.subs), topic.subs)
        case PrintToStandardOut(logLevel, logMode) => PrintToStandardOut(logLevel, logMode)
        case WriteToFile(path) => WriteToFile(path.subs)
        case PostToSlack(hookUrl, onlyPositiveMatchData, intervalSeconds) =>
          PostToSlack(hookUrl.subs, onlyPositiveMatchData, intervalSeconds)
        case StandingQueryResultOutputUserDef
              .CypherQuery(query, parameter, parallelism, andThen, allowAllNodeScan) =>
          StandingQueryResultOutputUserDef.CypherQuery(
            query.subs,
            parameter,
            parallelism,
            andThen.map(_.subs),
            allowAllNodeScan
          )
        case WriteToKinesis(
              credentials,
              streamName,
              format,
              kinesisParallelism,
              kinesisMaxBatchSize,
              kinesisMaxRecordsPerSecond,
              kinesisMaxBytesPerSecond
            ) =>
          WriteToKinesis(
            credentials.map(_.subs),
            streamName.subs,
            format,
            kinesisParallelism,
            kinesisMaxBatchSize,
            kinesisMaxRecordsPerSecond,
            kinesisMaxBytesPerSecond
          )
      }
    }
    // Return a copy of the recipe.
    // Selected fields are token substituted by invoking subs.
    recipe.copy(
      ingestStreams = recipe.ingestStreams map {
        case KafkaIngest(
              format,
              topics,
              parallelism,
              bootstrapServers,
              groupId,
              securityProtocol,
              autoCommitIntervalMs,
              autoOffsetReset,
              endingOffset,
              maximumPerSecond
            ) =>
          KafkaIngest(
            format,
            topics,
            parallelism,
            bootstrapServers.subs,
            groupId,
            securityProtocol,
            autoCommitIntervalMs,
            autoOffsetReset,
            endingOffset,
            maximumPerSecond
          )
        case KinesisIngest(
              format,
              streamName,
              shardIds,
              parallelism,
              credentials,
              iteratorType,
              numRetries,
              maximumPerSecond
            ) =>
          KinesisIngest(
            format,
            streamName.subs,
            shardIds,
            parallelism,
            credentials.map(_.subs),
            iteratorType,
            numRetries,
            maximumPerSecond
          )
        case ServerSentEventsIngest(format, url, parallelism, maximumPerSecond) =>
          ServerSentEventsIngest(format, url.subs, parallelism, maximumPerSecond)
        case SQSIngest(
              format,
              queueURL,
              readParallelism,
              writeParallelism,
              credentials,
              deleteReadMessages,
              maximumPerSecond
            ) =>
          SQSIngest(
            format,
            applySubstitution(queueURL, values),
            readParallelism,
            writeParallelism,
            credentials.map(_.subs),
            deleteReadMessages,
            maximumPerSecond
          )
        case FileIngest(
              format,
              path,
              encoding,
              parallelism,
              maximumLineSize,
              startAtOffset,
              ingestLimit,
              maximumPerSecond,
              fileIngestMode
            ) =>
          FileIngest(
            format,
            applySubstitution(path, values),
            encoding,
            parallelism,
            maximumLineSize,
            startAtOffset,
            ingestLimit,
            maximumPerSecond,
            fileIngestMode
          )
      },
      standingQueries = recipe.standingQueries.map(sq =>
        sq.copy(
          outputs = for { (k, v) <- sq.outputs } yield k -> v.subs // do not use mapValues! 💀
        )
      )
    )
  }

  /** Extremely simple token substitution language.
    *
    * If the first character in the input string equals '$', then the string
    * represents a token that is to be substituted.
    *
    * The token's value is read from the values map. If the value is not defined,
    * an error occurs.
    *
    * Internal substitutions are not supported.
    *
    * Double leading '$' characters ("$$") escapes token substitution and is
    * interpreted as a single leading '$'.
    */
  def applySubstitution(input: String, values: Map[String, String]): String =
    if (input.startsWith("$")) {
      val key = input.slice(1, input.length)
      if (input.startsWith("$$")) {
        key
      } else {
        values.getOrElse(key, sys.error(s"Missing required parameter $key; use --recipe-value $key="))
      }
    } else {
      input
    }

  /** Synchronously maps a recipe file name or URL to the Recipe it contains.
    * The Recipe is also validated.
    * Any errors are converted to a sequence of user facing messages.
    */
  def get(recipeFilenameOrUrl: String): Either[Seq[String], Recipe] =
    for {
      json <- Using(filenameOrUrl(recipeFilenameOrUrl).openStream())(parseToJson).toEither.left.map(e =>
        Seq(e.toString)
      )
      recipe <- Recipe.fromJson(json)
      validatedRecipe <- isCurrentVersion(recipe)
    } yield validatedRecipe

  private val currentRecipeDefinitionVersionNumber = 1
  private def isCurrentVersion(recipe: Recipe): Either[Seq[String], Recipe] = Either.cond(
    recipe.recipeDefinitionVersionNumber == currentRecipeDefinitionVersionNumber,
    recipe,
    Seq(s"The only supported recipe definition version number is $currentRecipeDefinitionVersionNumber")
  )
}
