package com.thatdot.quine.graph.cypher.quinepattern

import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Promise}

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.TestKit
import org.apache.pekko.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.graph.behavior.QuinePatternCommand
import com.thatdot.quine.graph.cypher.quinepattern.QueryPlan._
import com.thatdot.quine.graph.quinepattern.NonNodeActor
import com.thatdot.quine.graph.{GraphService, NamespaceId, QuineIdLongProvider, StandingQueryId, defaultNamespaceId}
import com.thatdot.quine.language.ast.{Expression, Source, Value}
import com.thatdot.quine.model.QuineValue
import com.thatdot.quine.persistor.{EventEffectOrder, InMemoryPersistor}

class TestPropertyAccess
    extends TestKit(ActorSystem("TestPropertyAccess"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {
  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  implicit val ec: ExecutionContext = system.dispatcher
  implicit val timeout: Timeout = Timeout(5.seconds)
  val namespace: NamespaceId = defaultNamespaceId
  val qidProvider: QuineIdLongProvider = QuineIdLongProvider()

  private val noSource: Source = Source.NoSource

  private def param(name: String): Expression =
    Expression.Parameter(noSource, Symbol("$" + name), None)

  def makeGraph(name: String): GraphService = Await.result(
    GraphService(
      name,
      effectOrder = EventEffectOrder.PersistorFirst,
      persistorMaker = InMemoryPersistor.persistorMaker,
      idProvider = qidProvider,
    )(LogConfig.permissive),
    5.seconds,
  )

  "Property access" should "work with direct plan construction (no Cypher parsing)" in {
    val graph = makeGraph("property-access-direct-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()

      // Set up a node with a property
      Await.result(graph.literalOps(namespace).setProp(nodeId, "name", QuineValue.Str("Alice")), 5.seconds)

      // Build a plan directly that:
      // 1. Anchors on the node
      // 2. Watches the "name" property (storing under alias "1.name")
      // 3. Watches the node ID (storing under binding "n")
      val plan = Anchor(
        AnchorTarget.Computed(param("nodeId")),
        Sequence(
          LocalProperty(Symbol("name"), aliasAs = Some(Symbol("1.name")), PropertyConstraint.Unconditional),
          LocalId(Symbol("n")),
          ContextFlow.Extend,
        ),
      )

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(Symbol("nodeId") -> Value.NodeId(nodeId))

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      results should have size 1
      val ctx = results.head

      // The property value should be stored under "1.name"
      ctx.bindings.get(Symbol("1.name")) match {
        case Some(Value.Text(s)) =>
          s shouldEqual "Alice"
        case Some(other) => fail(s"Expected Text, got: $other")
        case None => fail(s"Property binding '1.name' not found. Available: ${ctx.bindings.keys}")
      }

      // The node ID should be stored under "n"
      ctx.bindings.get(Symbol("n")) match {
        case Some(Value.NodeId(id)) => id shouldEqual nodeId
        case Some(other) => fail(s"Expected NodeId, got: $other")
        case None => fail(s"Node ID binding 'n' not found")
      }

    } finally Await.result(graph.shutdown(), 5.seconds)
  }

  it should "work in RETURN clause without UNION" in {
    val graph = makeGraph("property-access-test")
    while (!graph.isReady) Thread.sleep(10)

    try {
      val nodeId = qidProvider.newQid()

      // Set up a node with a property
      Await.result(graph.literalOps(namespace).setProp(nodeId, "name", QuineValue.Str("Alice")), 5.seconds)

      // Parse and plan a simple query with property access
      // $nodeId is a Cypher parameter placeholder, not Scala interpolation
      val cypherQuery: String =
        "MATCH (n) WHERE id(n) = $nodeId RETURN n.name AS name": @nowarn("msg=possible missing interpolator")
      val planned = QueryPlanner.planFromString(cypherQuery) match {
        case Right(p) => p
        case Left(error) => fail(s"Failed to plan query: $error")
      }

      val resultPromise = Promise[Seq[QueryContext]]()
      val outputTarget = OutputTarget.EagerCollector(resultPromise)
      val params = Map(Symbol("nodeId") -> Value.NodeId(nodeId))

      val loader = graph.system.actorOf(Props(new NonNodeActor(graph, namespace)))
      loader ! QuinePatternCommand.LoadQueryPlan(
        sqid = StandingQueryId.fresh(),
        plan = planned.plan,
        mode = RuntimeMode.Eager,
        params = params,
        namespace = namespace,
        output = outputTarget,
        returnColumns = planned.returnColumns,
        outputNameMapping = planned.outputNameMapping,
      )

      val results = Await.result(resultPromise.future, 10.seconds)

      results should have size 1

      // Extract any text values
      val textValues = results.head.bindings.values.collect { case Value.Text(s) => s }.toSet

      textValues should contain("Alice")

    } finally Await.result(graph.shutdown(), 5.seconds)
  }
}
