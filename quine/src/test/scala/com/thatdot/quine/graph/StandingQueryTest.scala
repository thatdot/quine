package com.thatdot.quine.graph

import java.io.{File, PrintStream}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.io.{BufferedSource, Source}
import scala.util.Random

import akka.actor.ActorSystem

import com.typesafe.scalalogging.Logger
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.interval
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import com.thatdot.quine.app._
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.{CypherQuery, WriteToFile}
import com.thatdot.quine.routes.{StandingQueryPattern => SqPattern, _}

class StandingQueryTest extends AnyFunSuite with BeforeAndAfterAll {

  val graph: GraphService = IngestTestGraph.makeGraph()
  implicit val system: ActorSystem = graph.system
  val ec: ExecutionContext = graph.shardDispatcherEC
  val quineApp = new QuineApp(graph)
  override def afterAll: Unit = {
    Await.result(graph.shutdown(), 1.second)
    Await.result(quineApp.shutdown(), 1.second)
  }

  /** Run a recipe and return a file that can be examined for output */
  def testRecipe(
    ingestConfig: IngestStreamConfiguration,
    sqPattern: SqPattern,
    sqOutputPattern: String
  ): File = {

    val tmp = File.createTempFile(Random.nextLong().toString, "sqtest")
    val statusLines = new StatusLines(Logger("StandingQueryTest"), new PrintStream(tmp))

    val standingQueryResultOutputUserDef: StandingQueryResultOutputUserDef =
      CypherQuery(sqOutputPattern, andThen = Some(WriteToFile(tmp.getAbsolutePath)))

    val sq: StandingQueryDefinition =
      StandingQueryDefinition(sqPattern, Map("results" -> standingQueryResultOutputUserDef))

    val recipe = Recipe(
      Recipe.currentVersion,
      "SQ_RECIPE",
      None,
      None,
      None,
      None,
      ingestStreams = List(ingestConfig),
      standingQueries = List(sq),
      statusQuery = None
    )

    RecipeInterpreter(statusLines, recipe, quineApp, graph, None)(graph.idProvider)
    tmp
  }

  test("Recipe correctly reads max long") {
    val ingestConfig = NumberIteratorIngest(
      FileIngestFormat.CypherLine(
        """WITH gen.node.from(toInteger($that)) AS n,
          |        toInteger($that) AS i
          |        MATCH (thisNode), (nextNode)
          |        WHERE id(thisNode) = id(n)
          |          AND id(nextNode) = idFrom(i + 1)
          |        SET thisNode.id = i
          |        SET nextNode.id=i+1
          |        CREATE (thisNode)-[:next]->(nextNode)
          |""".stripMargin
      ),
      startAt = 9223372036854775804L,
      ingestLimit = Some(6L),
      throttlePerSecond = None
    )

    val sqPattern = SqPattern.Cypher("""MATCH (a)-[:next]->(b)
        |        WHERE exists(a.id) AND exists(b.id)
        |        RETURN DISTINCT id(a) as id""".stripMargin)

    val sqOutputPattern =
      """MATCH (a)-[:next]->(b)
              WHERE id(a) = $that.data.id
              RETURN a.id, b.id"""
    /*
       Testing large values. One of the output values should contain the string
       "data":{"a.id":9223372036854775806,"b.id":9223372036854775807}
       When this is incorrectly rounded through ujson we get
       "data":{"a.id":9223372036854775807,"b.id":9223372036854775807}
     */
    val tmpFile = testRecipe(
      ingestConfig,
      sqPattern: SqPattern,
      sqOutputPattern
    )

    eventually(Eventually.timeout(5.seconds), interval(500.millis)) {
      val source: BufferedSource = Source.fromFile(tmpFile)
      val lines = source.getLines().toList
      try lines.exists(_.contains("""data":{"a.id":9223372036854775806,"b.id":9223372036854775807}""")) shouldBe true
      finally source.close
    }

  }
}
