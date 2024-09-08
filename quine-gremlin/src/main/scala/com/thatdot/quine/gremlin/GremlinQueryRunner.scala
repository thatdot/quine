package com.thatdot.quine.gremlin

import scala.reflect.{ClassTag, classTag}
import scala.util.matching.Regex

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.Timeout

import com.thatdot.quine.graph.{LiteralOpsGraph, NamespaceId}
import com.thatdot.quine.model.{Milliseconds, QuineValue}
import com.thatdot.quine.util.Log._

/** Entry point for running Gremlin queries on Quine.
  *
  * Example:
  * {{{
  *   // Usual setup for a Quine graph
  *   implicit val graph = GraphService(
  *     "quine-graph",
  *     persistor = EmptyPersistor()(_),
  *     idProvider = QuineUUIDProvider,
  *   )
  *   val ec = graph.system.dispatcher
  *   implicit val timeout = Timeout(10 seconds)
  *
  *   // Setup the Gremlin client
  *   val gremlin = GremlinQueryRunner(graph)
  *
  *   // Start running queries!
  *   val result: Future[Seq[Any]] = gremlin.query("g.V().has('foo').valueMap()")
  * }}}
  *
  * @param graph handle to the Quine graph on which queries are going to be run
  * @param customIdRegex a regex which should match the string representation of the custom IDs
  * @param customLiteralsParser the information needed to parse out a custom literal value
  */
final case class GremlinQueryRunner(
  graph: LiteralOpsGraph,
  customIdRegex: Regex = """#?[-a-zA-Z0-9]+""".r,
  customLiteralsParser: Option[(Regex, String => Option[QuineValue])] = None,
)(implicit
  protected val timeout: Timeout,
) extends GremlinTypes
    with GremlinParser {

  implicit val system: ActorSystem = graph.system

  private val lexer = new GremlinLexer(
    graph.idProvider,
    customIdRegex,
    customLiteralsParser,
  )

  /** Execute a Gremlin query on the graph and collect the results
    *
    * @param queryString the query to execute
    * @param parameters a mapping from free variables in queryString (as Symbols) to their values (as Any)
    * @param atTime moment in time to query ([[None]] represents the present)
    * @return back-pressured source of results from running the query
    */
  @throws[QuineGremlinException]("if the query fails to parse or the parameters can't be evaluated")
  def query(
    queryString: String,
    parameters: Map[Symbol, QuineValue] = Map.empty,
    namespace: NamespaceId = None,
    atTime: Option[Milliseconds] = None,
  )(implicit logConfig: LogConfig): Source[Any, NotUsed] = {
    val query: Query = parseQuery(new lexer.Scanner(queryString))
    val store =
      parameters.view.mapValues(TypedValue.apply).foldLeft(VariableStore.empty) { case (store, (name, value)) =>
        store + ((name, value.eval()(store, idProvider)))
      }
    query
      .run(store, namespace, atTime, logConfig: LogConfig)
      .named(s"gremlin-query-atTime-${atTime.fold("none")(_.millis.toString)}")
  }

  /** Execute a Gremlin query on the graph, collect the results, and cast them to the desired type */
  @throws[QuineGremlinException]("if the query fails to parse or the parameters can't be evaluated")
  def queryExpecting[T: ClassTag](
    queryString: String,
    parameters: Map[Symbol, QuineValue] = Map.empty,
    namespace: NamespaceId = None,
    atTime: Option[Milliseconds] = None,
  )(implicit logConfig: LogConfig): Source[T, NotUsed] = {
    val msg = "Top level query was required by the user to have a different type"
    query(queryString, parameters, namespace, atTime)
      .map(_.castTo[T](msg, None).get)
      .named(s"gremlin-query-as-${classTag[T].runtimeClass.getSimpleName}-${atTime.fold("none")(_.millis.toString)}")
  }
}
