package com.thatdot.quine.gremlin

import java.util.UUID

import scala.concurrent.Await

import com.thatdot.quine.model.{QuineId, QuineValue}

class SimpleQueries extends GremlinHarness("quine-simple-gremlin-queries") {

  val uuid1: UUID = UUID.fromString("00000000-0000-0000-0000-000000000001")
  val uuid2: UUID = UUID.fromString("00000000-0000-0000-0000-000000000002")
  val uuid3: UUID = UUID.fromString("00000000-0000-0000-0000-000000000003")
  val uuid4: UUID = UUID.fromString("00000000-0000-0000-0000-000000000004")
  val uuid5: UUID = UUID.fromString("00000000-0000-0000-0000-000000000005")

  def uuidToQid(uuid: UUID): QuineId = idProv.customIdToQid(uuid)
  val qid1: QuineId = uuidToQid(uuid1)
  val qid2: QuineId = uuidToQid(uuid2)
  val qid3: QuineId = uuidToQid(uuid3)
  val qid4: QuineId = uuidToQid(uuid4)
  val qid5: QuineId = uuidToQid(uuid5)

  override def beforeAll(): Unit = {
    super.beforeAll()

    implicit val ec = graph.system.dispatcher

    Await.result(
      for {
        // Set some properties
        _ <- literalOps.setProp(qid1, "foo", QuineValue.Integer(733L))
        _ <- literalOps.setProp(qid5, "foo", QuineValue.Integer(733L))

        _ <- literalOps.setProp(qid2, "baz", QuineValue.True)
        _ <- literalOps.setProp(qid3, "baz", QuineValue.Str("hello world"))

        listNums = QuineValue.List(Vector(1L, 2L, 3L).map(QuineValue.Integer(_)))
        _ <- literalOps.setProp(qid4, "qux", listNums)

        _ <- literalOps.setProp(qid4, "quux", QuineValue.Str("boa constrictor"))

        // Set some edges
        _ <- literalOps.addEdge(qid1, qid2, "edge1")
        _ <- literalOps.addEdge(qid2, qid4, "edge1")
        _ <- literalOps.addEdge(qid4, qid1, "edge1")

        _ <- literalOps.addEdge(qid4, qid5, "edge2")
        _ <- literalOps.addEdge(qid4, qid2, "edge2")

        _ <- literalOps.addEdge(qid1, qid3, "edge3")
        _ <- literalOps.addEdge(qid4, qid3, "edge3")
      } yield (),
      timeout.duration,
    )
  }

  test("`.V(...)` traversal step") {
    testQuery(
      "g.V([])",
      Seq.empty,
    )

    testQuery(
      "g.V()",
      Seq(Vertex(qid1), Vertex(qid2), Vertex(qid3), Vertex(qid4), Vertex(qid5)),
      ordered = false,
    )

    testQuery(
      "g.V(00000000-0000-0000-0000-000000000001)",
      Seq(Vertex(qid1)),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000004
        |   , 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002 )
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid4), Vertex(qid1), Vertex(qid2)),
    )

    testQuery(
      """g.V([ 00000000-0000-0000-0000-000000000004
        |    , 00000000-0000-0000-0000-000000000001
        |    , 00000000-0000-0000-0000-000000000002 ])
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid4), Vertex(qid1), Vertex(qid2)),
    )

  }

  test("`.recentV(...)` traversal step") {
    testQuery(
      "g.V(recent_nodes)",
      Seq(Vertex(qid1), Vertex(qid2), Vertex(qid3), Vertex(qid4), Vertex(qid5)),
      ordered = false,
    )

    testQuery(
      "g.recentV(1).count()",
      Seq(1),
    )
  }

  test("`.values(...)` traversal step") {
    testQuery(
      "g.V(00000000-0000-0000-0000-000000000001).values('foo')",
      Seq(733),
    )

    testQuery(
      "g.V(00000000-0000-0000-0000-000000000001).values('bar')",
      Seq.empty,
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000004
        |   , 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002 )
        | .values('qux')
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(List(1, 2, 3)),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000003
        |   , 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002 )
        | .values('baz')
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq("hello world", true),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000003
        |   , 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002 )
        | .values()
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq("hello world", 733, true),
    )
  }

  test("`.valueMap(...)` traversal step") {
    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000003
        |   , 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002 )
        | .valueMap('foo')
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Map(), Map("foo" -> 733), Map()),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000003
        |   , 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002 )
        | .valueMap()
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Map("baz" -> "hello world"), Map("foo" -> 733), Map("baz" -> true)),
    )
  }

  test("`.is(...)` traversal step") {
    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000003
        |   , 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002 )
        | .id()
        | .is(00000000-0000-0000-0000-000000000001)
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(uuid1),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000003
        |   , 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002 )
        | .values("foo")
        | .is(within([1,2,733]))
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(733),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000003
        |   , 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002 )
        | .values("foo")
        | .is(within([1,2,73]))
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(),
    )
  }

  test("`.dedup()` traversal step") {
    testQuery(
      "g.V().dedup()",
      Seq(Vertex(qid1), Vertex(qid2), Vertex(qid3), Vertex(qid4), Vertex(qid5)),
      ordered = false,
    )

    testQuery(
      "g.V([]).dedup()",
      Seq.empty,
    )

    testQuery(
      "g.V(00000000-0000-0000-0000-000000000001).values('bar').dedup()",
      Seq.empty,
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000004
        |   , 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002 )
        | .dedup()
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid4), Vertex(qid1), Vertex(qid2)),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000005
        |   , 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000005 )
        | .dedup()
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid5), Vertex(qid1)),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000005
        |   , 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000005 )
        | .values('foo')
        | .dedup()
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(733),
    )
  }

  test("`.eqToVar(...)` traversal step") {
    testQuery(
      "g.V().has('foo').as('x').eqToVar('x')",
      Seq(Vertex(qid5), Vertex(qid1)),
      ordered = false,
    )

    testQuery(
      """g.V(00000000-0000-0000-0000-000000000001)
        | .as('v').values('foo').as('x').select('v')
        | .both()
        | .both()
        | .both()
        | .as('y')
        | .values('foo').eqToVar('x').select('y')
        """.stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid5), Vertex(qid1)),
      ordered = false,
    )
  }

  test("`.has(...)` traversal step") {
    testQuery(
      "g.V().has('foo')",
      Seq(Vertex(qid5), Vertex(qid1)),
      ordered = false,
    )

    testQuery(
      "g.V(00000000-0000-0000-0000-000000000001).has('bar')",
      Seq.empty,
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002
        |   , 00000000-0000-0000-0000-000000000003 )
        | .has('baz')
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid2), Vertex(qid3)),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000005
        |   , 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000005 )
        | .has('foo')
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid5), Vertex(qid1), Vertex(qid5)),
    )

    testQuery(
      "g.V(00000000-0000-0000-0000-000000000005).has('foo', 733)",
      Seq(Vertex(qid5)),
    )

    testQuery(
      "g.V(00000000-0000-0000-0000-000000000005).has('foo', neq(733))",
      Seq(),
    )

    testQuery(
      raw"g.V(00000000-0000-0000-0000-000000000003).has('baz', regex('\w+\s\w+'))",
      Seq(Vertex(qid3)),
    )

    testQuery(
      raw"g.V(00000000-0000-0000-0000-000000000003).has('baz', regex('\w+'))",
      Seq(),
    )
  }

  test("`.hasNot(...)` traversal step") {
    testQuery(
      "g.V(00000000-0000-0000-0000-000000000001).hasNot('bar')",
      Seq(Vertex(qid1)),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002
        |   , 00000000-0000-0000-0000-000000000003 )
        | .hasNot('baz')
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid1)),
    )
  }

  test("`.out(...)` traversal step") {
    testQuery(
      "g.V().out('edge1')",
      Seq(Vertex(qid1), Vertex(qid2), Vertex(qid4)),
      ordered = false,
    )

    testQuery(
      "g.V(00000000-0000-0000-0000-000000000001).out('edge1')",
      Seq(Vertex(qid2)),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002
        |   , 00000000-0000-0000-0000-000000000003 )
        | .out('edge1')
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid2), Vertex(qid4)),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002
        |   , 00000000-0000-0000-0000-000000000004 )
        | .out('edge1','edge3')
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid2), Vertex(qid3), Vertex(qid4), Vertex(qid1), Vertex(qid3)),
    )

    testQuery(
      """g.V(00000000-0000-0000-0000-000000000001)
        | .out()
        | .out()
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid4)),
    )

    testQuery(
      "g.V(00000000-0000-0000-0000-000000000001).out().out().out()",
      Seq(Vertex(qid1), Vertex(qid2), Vertex(qid3), Vertex(qid5)),
      ordered = false,
    )
  }

  test("`.outLimit(...)` traversal step") {
    testQuery(
      "g.V().out('edge1')",
      Seq(Vertex(qid1), Vertex(qid2), Vertex(qid4)),
      ordered = false,
    )

    testQuery(
      "g.V(00000000-0000-0000-0000-000000000001).outLimit('edge1',9)",
      Seq(Vertex(qid2)),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002
        |   , 00000000-0000-0000-0000-000000000003 )
        | .outLimit('edge1',1)
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid2)),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002
        |   , 00000000-0000-0000-0000-000000000004 )
        | .outLimit('edge1','edge3',2)
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid2), Vertex(qid3)),
    )
  }

  test("`.in(...)` traversal step") {
    testQuery(
      "g.V(00000000-0000-0000-0000-000000000001).out().in()",
      Seq(Vertex(qid1), Vertex(qid4)),
      ordered = false,
    )
  }

  test("`.both(...)` traversal step") {
    testQuery(
      "g.V(00000000-0000-0000-0000-000000000001).both('edge1')",
      Seq(Vertex(qid2), Vertex(qid4)),
      ordered = false,
    )
  }

  test("`.groupCount(...)` traversal step") {
    testQuery(
      "g.V(00000000-0000-0000-0000-000000000001).both().both().both().groupCount()",
      Seq(
        Map(
          Vertex(qid1) -> 6,
          Vertex(qid2) -> 9,
          Vertex(qid3) -> 6,
          Vertex(qid4) -> 9,
          Vertex(qid5) -> 3,
        ),
      ),
    )
  }

  test("`.count(...)` traversal step") {
    testQuery(
      "g.V(00000000-0000-0000-0000-000000000001).both().both().both().count()",
      Seq(6 + 9 + 6 + 9 + 3),
    )
  }

  test("`.limit(...)` traversal step") {
    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002
        |   , 00000000-0000-0000-0000-000000000004 )
        | .out('edge1','edge3')
        | .limit(3)
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid2), Vertex(qid3), Vertex(qid4)),
    )
  }

  test("`.id(...)` traversal step") {
    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002
        |   , 00000000-0000-0000-0000-000000000004 )
        | .id()
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(uuid1, uuid2, uuid4),
    )
  }

  test("`.unrollPath(...)` traversal step") {
    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000005 )
        | .out('edge1')
        | .out('edge1')
        | .out('edge3')
        | .unrollPath()
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid1), Vertex(qid2), Vertex(qid4), Vertex(qid3)),
    )
  }

  test("`.outE(...)`/`.inE(...)`/`.bothE(...)` traversal steps") {
    testQuery(
      "g.V(00000000-0000-0000-0000-000000000002).outE('edge1')",
      Seq(Edge(qid2, Symbol("edge1"), qid4)),
    )

    testQuery(
      "g.V(00000000-0000-0000-0000-000000000002).inE('edge1')",
      Seq(Edge(qid1, Symbol("edge1"), qid2)),
    )

    testQuery(
      "g.V(00000000-0000-0000-0000-000000000002).bothE('edge1')",
      Seq(Edge(qid2, Symbol("edge1"), qid4), Edge(qid1, Symbol("edge1"), qid2)),
      ordered = false,
    )
  }

  test("`.outV(...)`/`.inV(...)`/`.bothV(...)` traversal steps") {
    testQuery(
      "g.V(00000000-0000-0000-0000-000000000002).bothE('edge1').inV()",
      Seq(Vertex(qid4), Vertex(qid2)),
      ordered = false,
    )

    testQuery(
      "g.V(00000000-0000-0000-0000-000000000002).bothE('edge1').outV()",
      Seq(Vertex(qid2), Vertex(qid1)),
      ordered = false,
    )

    testQuery(
      "g.V(00000000-0000-0000-0000-000000000002).bothE('edge1').bothV()",
      Seq(Vertex(qid4), Vertex(qid2), Vertex(qid2), Vertex(qid1)),
      ordered = false,
    )
  }

  test("`.not(...)`/`.where(...)` traversal steps") {
    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002)
        | .not(_.has('foo'))
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid2)),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002 )
        | .where(_.has('foo'))
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid1)),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002 )
        | .not(_.out().has('qux'))
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid1)),
    )
  }

  test("`.and(...)`/`.or(..)` traversal steps") {
    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002 )
        | .or(_.out('edge3'), _.in('edge1'))
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid1), Vertex(qid2)),
    )

    testQuery(
      "g.V(00000000-0000-0000-0000-000000000001).and(in(), out().out())",
      Seq(Vertex(qid1)),
    )
  }

  test("`.hasId(..)` traversal step") {
    testQuery(
      """g.V(00000000-0000-0000-0000-000000000001)
        | .out('edge1', 'edge2')
        | .out('edge1', 'edge2')
        | .out('edge1', 'edge2')
        | .hasId( 00000000-0000-0000-0000-000000000002
        |       , 00000000-0000-0000-0000-000000000005 )
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid5), Vertex(qid2)),
      ordered = false,
    )
  }

  test("`.as(...)`/`.select(...)` traversal step") {

    testQuery(
      "g.V(00000000-0000-0000-0000-000000000001).as('x').out().select('x')",
      Seq(Vertex(qid1), Vertex(qid1)),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002
        |   , 00000000-0000-0000-0000-000000000003 )
        | .as('x')
        | .out('edge1')
        | .select('x')
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid1), Vertex(qid2)),
    )

    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002
        |   , 00000000-0000-0000-0000-000000000004 )
        | .as('x')
        | .out('edge1','edge3')
        | .as('y')
        | .select('x','y')
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(
        Map("x" -> Vertex(qid1), "y" -> Vertex(qid2)),
        Map("x" -> Vertex(qid1), "y" -> Vertex(qid3)),
        Map("x" -> Vertex(qid2), "y" -> Vertex(qid4)),
        Map("x" -> Vertex(qid4), "y" -> Vertex(qid1)),
        Map("x" -> Vertex(qid4), "y" -> Vertex(qid3)),
      ),
    )
  }

  test("`.union(..)` traversal step") {
    testQuery(
      """g.V( 00000000-0000-0000-0000-000000000001
        |   , 00000000-0000-0000-0000-000000000002
        |   , 00000000-0000-0000-0000-000000000004 )
        | .union(_.out('edge1'),_.out('edge3'))
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(Vertex(qid2), Vertex(qid3), Vertex(qid4), Vertex(qid1), Vertex(qid3)),
    )
  }

  test("query level variables") {

    testQuery(
      """x = 00000000-0000-0000-0000-000000000004;
         |y = 00000000-0000-0000-0000-000000000001;
         |z = 00000000-0000-0000-0000-000000000002;
         |g.V(x,y,z)
         | .out()
         | .values('baz', 'foo')
         | .dedup()
         |""".stripMargin.filterNot(_.isWhitespace),
      Seq(733, true, "hello world"),
      ordered = false,
    )

    testQuery(
      "x = []; g.V(x).dedup()",
      Seq.empty,
    )

    testQuery(
      """xs = [ 00000000-0000-0000-0000-000000000004
        |     , 00000000-0000-0000-0000-000000000001
        |     , 00000000-0000-0000-0000-000000000002 ];
        |g.V(xs)
        | .out()
        | .values('baz', 'foo')
        | .dedup()
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(733, true, "hello world"),
      ordered = false,
    )
  }

  test("query parameters") {

    testQuery(
      "g.V().has(fooVar)",
      Seq(Vertex(qid5), Vertex(qid1)),
      parameters = Map(Symbol("fooVar") -> QuineValue.Str("foo")),
      ordered = false,
    )

    testQuery(
      "g.V(x,y,z).out().values('baz', 'foo').dedup()",
      Seq(733, true, "hello world"),
      parameters = Map(
        Symbol("x") -> QuineValue.Id(uuid4),
        Symbol("y") -> QuineValue.Id(uuid1),
        Symbol("z") -> QuineValue.Id(uuid2),
      ),
      ordered = false,
    )

    testQuery(
      "g.V(x).dedup()",
      Seq.empty,
      parameters = Map(Symbol("x") -> QuineValue.List(Vector.empty)),
    )

    testQuery(
      "g.V(xs).out().values('baz', 'foo').dedup()",
      Seq(733, true, "hello world"),
      parameters = Map(
        Symbol("xs") -> QuineValue.List(Vector(uuid4, uuid1, uuid2).map(QuineValue.Id(_))),
      ),
      ordered = false,
    )
  }

  test("query variables have precedence over equivalently-named parameters") {
    testQuery(
      "x='baz'; g.V().has(x)",
      Seq(Vertex(qid2), Vertex(qid3)),
      parameters = Map(Symbol("x") -> QuineValue.Str("foo")),
      ordered = false,
    )

    testQuery(
      """ids = [ 00000000-0000-0000-0000-000000000004
        |     , 00000000-0000-0000-0000-000000000001
        |     , 00000000-0000-0000-0000-000000000002 ];
        |g.V(ids)
        | .out()
        | .values('baz', 'foo')
        | .dedup()
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(733, true, "hello world"),
      parameters = Map(
        Symbol("ids") -> QuineValue.List(Vector(uuid3, uuid1, uuid2).map(QuineValue.Id(_))),
      ),
      ordered = false,
    )
  }

  test("shortcutting behaviour of `.limit(...)`") {

    // This should be a lot of nodes, yet limit still works efficiently
    testQuery(
      """xs = [ 00000000-0000-0000-0000-000000000004
        |     , 00000000-0000-0000-0000-000000000001
        |     , 00000000-0000-0000-0000-000000000002 ];
        |g.V(xs).both().both().both().both().both().both().both().both().both()
        |       .both().both().both().both().both().both().both().both().both()
        |       .both().both().both().both().both().both().both().both().both()
        |       .both().both().both().both().both().both().both().both().both()
        |       .both().both().both().both().both().both().both().both().both()
        |       .both().both().both().both().both().both().both().both().both()
        |       .or(_.both().both().both().both().both().both().both().both().both()
        |          ,_.both().both().both().both().both().both().both().both().both()
        |          ,_.both().both().both().both().both().both().both().both().both()
        |          ,_.both().both().both().both().both().both().both().both().both())
        |       .both().both().both().both().both().both().both().both().both()
        |       .both().both().both().both().both().both().both().both().both()
        |       .both().both().both().both().both().both().both().both().both()
        |       .limit(2)
        |       .count()
        |""".stripMargin.filterNot(_.isWhitespace),
      Seq(2),
      ordered = true,
    )
  }
}
