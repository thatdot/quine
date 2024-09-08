package com.thatdot.quine.gremlin

import java.util.UUID

import scala.concurrent.Await

import com.thatdot.quine.model.{QuineId, QuineValue}

class ErrorMessages extends GremlinHarness("quine-simple-gremlin-errors") {

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
        _ <- literalOps.setPropBytes(qid2, "box", Array(0xDE.toByte, 0xAD.toByte, 0xBE.toByte, 0xEF.toByte))
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

  test("Parse errors") {
    interceptQuery(
      "g..V()",
      """ParseError at 1.3: malformed traversal step
        |
        |g..V()
        |  ^""".stripMargin,
    )

    interceptQuery(
      "g.V().out().in(.values()",
      """ParseError at 1.16: ')' expected but . found
        |
        |g.V().out().in(.values()
        |               ^""".stripMargin,
    )

    interceptQuery(
      "g.V(12ds3)",
      """ParseError at 1.7: malformed traversal step
        |
        |g.V(12ds3)
        |      ^""".stripMargin,
    )

    interceptQuery(
      "g.V()+.in()",
      """LexicalError at 1.6: syntax error
        |
        |g.V()+.in()
        |     ^""".stripMargin,
    )
  }

  test("Unbound variable errors (at the query level)") {
    interceptQuery(
      "x1 = [1,2,3]; g.V(x2)",
      """UnboundVariableError at 1.19: x2 is unbound
        |
        |x1 = [1,2,3]; g.V(x2)
        |                  ^""".stripMargin,
    )
  }

  test("Unbound variable errors (at the traversal level)") {
    interceptQuery(
      "g.V(00000000-0000-0000-0000-000000000001).select('x')",
      """UnboundVariableError at 1.50: x is unbound
        |
        |g.V(00000000-0000-0000-0000-000000000001).select('x')
        |                                                 ^""".stripMargin,
    )
  }

  test("Type errors") {
    interceptQuery(
      "g.V([]).out('foo',123,'bar')",
      """TypeMismatchError at 1.19: `.out(...)` requires its arguments to be strings
        |  expected class java.lang.String
        |  but got  class java.lang.Long
        |
        |g.V([]).out('foo',123,'bar')
        |                  ^""".stripMargin,
    )

    interceptQuery(
      "g.V(00000000-0000-0000-0000-000000000001,2,3)",
      """TypeMismatchError at 1.42: `.V(...)` requires its arguments to be ids, but 2 was not
        |  expected class java.util.UUID
        |  but got  class java.lang.Long
        |
        |g.V(00000000-0000-0000-0000-000000000001,2,3)
        |                                         ^""".stripMargin,
    )

    interceptQuery(
      "g.V(00000000-0000-0000-0000-000000000001).is(regex(123))",
      """TypeMismatchError at 1.52: `regex(...)` expects its argument to be a string
        |  expected class java.lang.String
        |  but got  class java.lang.Long
        |
        |g.V(00000000-0000-0000-0000-000000000001).is(regex(123))
        |                                                   ^""".stripMargin,
    )

    interceptQuery(
      "g.V(00000000-0000-0000-0000-000000000001).values('foo').is(regex(\".*\"))",
      """TypeMismatchError at 1.57: the predicate `regex(".*")` can't be used on `733`
        |  expected class java.lang.String
        |  but got  class java.lang.Long
        |
        |g.V(00000000-0000-0000-0000-000000000001).values('foo').is(regex(".*"))
        |                                                        ^""".stripMargin,
    )
  }

  test("Deserialization errors") {
    interceptQuery(
      "g.V(00000000-0000-0000-0000-000000000002).values('box')",
      """FailedDeserializationError at 1.43: property `box` could not be unpickled.
        |  Raw bytes: DEADBEEF
        |
        |g.V(00000000-0000-0000-0000-000000000002).values('box')
        |                                          ^""".stripMargin,
    )
  }
}
