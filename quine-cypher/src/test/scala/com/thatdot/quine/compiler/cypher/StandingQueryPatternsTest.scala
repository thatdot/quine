package com.thatdot.quine.compiler.cypher

import cats.data.NonEmptyList
import org.scalactic.source
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec

import com.thatdot.quine.graph
import com.thatdot.quine.graph.cypher.{CypherException, Expr, Func, Position, SourceText}
import com.thatdot.quine.graph.{GraphQueryPattern, QuineIdRandomLongProvider, WithExplicitPositions, idFrom}
import com.thatdot.quine.model.{PositionAwareIdProvider, QuineValue}

class StandingQueryPatternsTest extends AnyFunSpec {
  import GraphQueryPattern._

  implicit val idProvider: QuineIdRandomLongProvider.type = QuineIdRandomLongProvider

  def testQuery(
    queryText: String,
    expected: GraphQueryPattern,
    skip: Boolean = false
  )(implicit
    pos: source.Position
  ): Unit = {
    def theTest(): Assertion = {
      val compiled = compileStandingQueryGraphPattern(queryText)
      assert(compiled === expected)
    }
    if (skip)
      ignore(queryText)(theTest())(pos)
    else
      it(queryText)(theTest())(pos)
  }

  /** Check that compiling a given standing query fails with the given exception.
    *
    * @param queryText query whose output we are checking
    * @param expected exception that we expect to intercept
    * @param pos source position of the call to `interceptQuery`
    * @param manifest information about the exception type we expect
    */
  def interceptQuery[T <: AnyRef](
    queryText: String,
    expected: T,
    skip: Boolean = false
  )(implicit
    pos: source.Position,
    manifest: Manifest[T]
  ): Unit = {
    def theTest(): Assertion = {
      val actual = intercept[T] {
        compileStandingQueryGraphPattern(queryText)
      }
      assert(actual == expected, "exception must match")
    }

    if (skip)
      ignore(queryText + " doesn't compile")(theTest())(pos)
    else
      it(queryText + " doesn't compile")(theTest())(pos)
  }

  describe("ID constraints in `WHERE`") {
    // valid id() in where condition
    testQuery(
      "MATCH (n) WHERE id(n) = 50 RETURN id(n)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            Some(idProvider.customIdToQid(50L)),
            Map.empty
          )
        ),
        List(),
        NodePatternId(0),
        Seq(ReturnColumn.Id(NodePatternId(0), false, Symbol("id(n)"))),
        None,
        Nil,
        distinct = false
      )
    )

    // valid DISTINCT id() in where condition
    testQuery(
      "MATCH (n) WHERE id(n) = 50 RETURN DISTINCT id(n)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            Some(idProvider.customIdToQid(50L)),
            Map.empty
          )
        ),
        List(),
        NodePatternId(0),
        Seq(ReturnColumn.Id(NodePatternId(0), false, Symbol("id(n)"))),
        None,
        Nil,
        distinct = true
      )
    )

    // valid strId() in where condition
    testQuery(
      "MATCH (n) WHERE strId(n) = '99' RETURN strId(n)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            Some(idProvider.customIdToQid(99L)),
            Map.empty
          )
        ),
        List(),
        NodePatternId(0),
        Seq(ReturnColumn.Id(NodePatternId(0), true, Symbol("strId(n)"))),
        None,
        Nil,
        distinct = false
      )
    )

    // multiple non-conflicting id() in where condition
    testQuery(
      "MATCH (n) WHERE id(n) = 50 AND id(n) = 50 RETURN DISTINCT id(n)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            Some(idProvider.customIdToQid(50L)),
            Map.empty
          )
        ),
        List(),
        NodePatternId(0),
        Seq(ReturnColumn.Id(NodePatternId(0), false, Symbol("id(n)"))),
        None,
        Nil,
        distinct = true
      )
    )

    // multiple non-conflicting strId() in where condition
    testQuery(
      "MATCH (n) WHERE strId(n) = '99' AND strId(n) = '99' RETURN DISTINCT strId(n)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            Some(idProvider.customIdToQid(99L)),
            Map.empty
          )
        ),
        List(),
        NodePatternId(0),
        Seq(ReturnColumn.Id(NodePatternId(0), true, Symbol("strId(n)"))),
        None,
        Nil,
        distinct = true
      )
    )

    // multiple non-conflicting heterogenous id constraints in where condition
    testQuery(
      "MATCH (n) WHERE strId(n) = '100' AND id(n) = 100 RETURN DISTINCT id(n)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            Some(idProvider.customIdToQid(100L)),
            Map.empty
          )
        ),
        List(),
        NodePatternId(0),
        Seq(ReturnColumn.Id(NodePatternId(0), false, Symbol("id(n)"))),
        None,
        Nil,
        distinct = true
      )
    )

    // idFrom-based id constraint
    testQuery(
      "MATCH (n) WHERE id(n) = idFrom(-1) RETURN DISTINCT id(n)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            Some(idFrom(Expr.Integer(-1))),
            Map.empty
          )
        ),
        List(),
        NodePatternId(0),
        Seq(ReturnColumn.Id(NodePatternId(0), false, Symbol("id(n)"))),
        None,
        Nil,
        distinct = true
      )
    )

    // idFrom-based strid constraint
    testQuery(
      "MATCH (n) WHERE strId(n) = idFrom('hello', 'world') RETURN DISTINCT strId(n)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            Some(idFrom(Expr.Str("hello"), Expr.Str("world"))),
            Map.empty
          )
        ),
        List(),
        NodePatternId(0),
        Seq(ReturnColumn.Id(NodePatternId(0), true, Symbol("strId(n)"))),
        None,
        Nil,
        distinct = true
      )
    )

    // locIdFrom-based strid constraint
    it("can use locIdFrom in an id constraint given a NamespacedIdProvider") {
      val namespacedIdProvider: PositionAwareIdProvider = WithExplicitPositions(idProvider)
      val customId =
        namespacedIdProvider.hashedCustomIdAtPositionIndex(10, graph.hashOfCypherValues(Seq(Expr.Integer(101))))
      val id = namespacedIdProvider.customIdToQid(customId)

      val compiled = compileStandingQueryGraphPattern(
        "MATCH (n) WHERE strId(n) = locIdFrom(10, 101) RETURN DISTINCT strId(n)"
      )(namespacedIdProvider)
      assert(
        compiled ===
          GraphQueryPattern(
            NonEmptyList.of(
              NodePattern(
                NodePatternId(0),
                Set(),
                Some(id),
                Map.empty
              )
            ),
            List(),
            NodePatternId(0),
            Seq(ReturnColumn.Id(NodePatternId(0), true, Symbol("strId(n)"))),
            None,
            Nil,
            distinct = true
          )
      )
    }

    // locId without an appropriate idProvider
    interceptQuery(
      "MATCH (n) WHERE strId(n) = locIdFrom('partitioned', '0118 999 881 999 119 7253') RETURN DISTINCT strId(n)",
      CypherException.ConstraintViolation(
        s"Unable to use a non-namespaced ID provider ($idProvider) with a namespace-dependent function locIdFrom",
        None
      )
    )
  }

  describe("Filtering with `WHERE` and mapping with `RETURN`") {
    // invalid id() in where condition gets downgraded into a filter
    testQuery(
      "MATCH (n) WHERE id(n) = 'hello' RETURN id(n)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            None,
            Map.empty
          )
        ),
        List(),
        NodePatternId(0),
        Seq(ReturnColumn.Id(NodePatternId(0), false, Symbol("id(n)"))),
        Some(Expr.Equal(Expr.Variable(Symbol("id(n)")), Expr.Str("hello"))),
        Nil,
        distinct = false
      )
    )

    // id() equality constraints turn into filters
    testQuery(
      "MATCH (n)-[:foo]->(m)-[:bar]->(o) WHERE id(n) <> id(o) RETURN id(m)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            None,
            Map.empty
          ),
          NodePattern(
            NodePatternId(1),
            Set(),
            None,
            Map.empty
          ),
          NodePattern(
            NodePatternId(2),
            Set(),
            None,
            Map.empty
          )
        ),
        List(
          EdgePattern(NodePatternId(0), NodePatternId(1), true, Symbol("foo")),
          EdgePattern(NodePatternId(1), NodePatternId(2), true, Symbol("bar"))
        ),
        NodePatternId(0),
        Seq(
          ReturnColumn.Id(NodePatternId(1), false, Symbol("id(m)")),
          ReturnColumn.Id(NodePatternId(0), false, Symbol("anon_0")),
          ReturnColumn.Id(NodePatternId(2), false, Symbol("anon_1"))
        ),
        Some(
          Expr.Not(Expr.Equal(Expr.Variable(Symbol("anon_0")), Expr.Variable(Symbol("anon_1"))))
        ),
        Seq(Symbol("id(m)") -> Expr.Variable(Symbol("id(m)"))),
        distinct = false
      )
    )

    // UDFs in filters or returns
    testQuery(
      "MATCH (n)-[:foo]->(m)-[:bar]->(o) WHERE parseJson(n.jsonField).baz = o.quz RETURN bytes(m.qux)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            None,
            Map.empty
          ),
          NodePattern(
            NodePatternId(1),
            Set(),
            None,
            Map.empty
          ),
          NodePattern(
            NodePatternId(2),
            Set(),
            None,
            Map.empty
          )
        ),
        List(
          EdgePattern(NodePatternId(0), NodePatternId(1), true, Symbol("foo")),
          EdgePattern(NodePatternId(1), NodePatternId(2), true, Symbol("bar"))
        ),
        NodePatternId(0),
        Seq(
          ReturnColumn.Property(NodePatternId(1), Symbol("qux"), Symbol("anon_0")),
          ReturnColumn.Property(NodePatternId(2), Symbol("quz"), Symbol("anon_2")),
          ReturnColumn.Property(NodePatternId(0), Symbol("jsonField"), Symbol("anon_1"))
        ),
        Some(
          Expr.Equal(
            Expr.Property(
              Expr.Function(
                Func.UserDefined("parseJson"),
                Vector(Expr.Variable(Symbol("anon_1")))
              ),
              Symbol("baz")
            ),
            Expr.Variable(Symbol("anon_2"))
          )
        ),
        Seq(
          Symbol("bytes(m.qux)") -> Expr.Function(
            Func.UserDefined("bytes"),
            Vector(Expr.Variable(Symbol("anon_0")))
          )
        ),
        distinct = false
      )
    )

    // invalid strId() in where condition gets downgraded to filter
    interceptQuery(
      "MATCH (n) WHERE strId(n) = 'hello' RETURN id(n)",
      CypherException.Compile("", None),
      skip = true
    )

    // conflicting id()s in where condition
    testQuery(
      "MATCH (n) WHERE id(n) = 22 AND id(n) = 23 RETURN id(n)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            Some(idProvider.customIdToQid(22L)),
            Map.empty
          )
        ),
        List(),
        NodePatternId(0),
        Seq(ReturnColumn.Id(NodePatternId(0), false, Symbol("id(n)"))),
        Some(Expr.Equal(Expr.Variable(Symbol("id(n)")), Expr.Integer(23L))),
        Nil,
        distinct = false
      )
    )
  }

  testQuery(
    "MATCH (n { foo: \"bar\" }) return id(n)",
    GraphQueryPattern(
      NonEmptyList.of(
        NodePattern(
          NodePatternId(0),
          Set(),
          None,
          Map(Symbol("foo") -> PropertyValuePattern.Value(QuineValue.Str("bar")))
        )
      ),
      List(),
      NodePatternId(0),
      Seq(ReturnColumn.Id(NodePatternId(0), false, Symbol("id(n)"))),
      None,
      Nil,
      distinct = false
    )
  )

  describe("Returning `id` and `strId`") {
    testQuery(
      "MATCH (n) return n.name, id(n), strId(n)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            None,
            Map.empty
          )
        ),
        List(),
        NodePatternId(0),
        Seq(
          ReturnColumn.Property(NodePatternId(0), Symbol("name"), Symbol("n.name")),
          ReturnColumn.Id(NodePatternId(0), false, Symbol("id(n)")),
          ReturnColumn.Id(NodePatternId(0), true, Symbol("strId(n)"))
        ),
        None,
        Nil,
        distinct = false
      )
    )
  }

  describe("Returning aliased column(s)") {
    testQuery(
      "MATCH (n) WHERE n.foo = 'bar' RETURN id(n) AS idN",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            None,
            Map(Symbol("foo") -> PropertyValuePattern.Value(QuineValue.Str("bar")))
          )
        ),
        List(),
        NodePatternId(0),
        Seq(ReturnColumn.Id(NodePatternId(0), false, Symbol("idN"))),
        None,
        Nil,
        distinct = false
      )
    )

    testQuery(
      "MATCH (n) WHERE n.foo = 'bar' RETURN DISTINCT id(n) AS idN",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            None,
            Map(Symbol("foo") -> PropertyValuePattern.Value(QuineValue.Str("bar")))
          )
        ),
        List(),
        NodePatternId(0),
        Seq(ReturnColumn.Id(NodePatternId(0), false, Symbol("idN"))),
        None,
        Nil,
        distinct = true
      )
    )
  }

  describe("Different ways to `MATCH` properties") {
    testQuery(
      "MATCH (n { foo: \"bar\" }) RETURN id(n) AS n",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            None,
            Map(Symbol("foo") -> PropertyValuePattern.Value(QuineValue.Str("bar")))
          )
        ),
        List(),
        NodePatternId(0),
        Seq(ReturnColumn.Id(NodePatternId(0), false, Symbol("n"))),
        None,
        Nil,
        distinct = false
      )
    )

    testQuery(
      "MATCH (n:Person { name: \"Joe\" }) RETURN id(n)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(Symbol("Person")),
            None,
            Map(Symbol("name") -> PropertyValuePattern.Value(QuineValue.Str("Joe")))
          )
        ),
        List(),
        NodePatternId(0),
        Seq(ReturnColumn.Id(NodePatternId(0), false, Symbol("id(n)"))),
        None,
        Nil,
        distinct = false
      )
    )

    testQuery(
      "MATCH (n { baz: 7.0 })-[:bar]->(m)<-[:foo]-({ foo: \"BAR\" }) where m.name IS NOT NULL RETURN id(m)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            None,
            Map(Symbol("baz") -> PropertyValuePattern.Value(QuineValue.Floating(7.0)))
          ),
          NodePattern(
            NodePatternId(1),
            Set(),
            None,
            Map(Symbol("name") -> PropertyValuePattern.AnyValue)
          ),
          NodePattern(
            NodePatternId(2),
            Set(),
            None,
            Map(Symbol("foo") -> PropertyValuePattern.Value(QuineValue.Str("BAR")))
          )
        ),
        List(
          EdgePattern(NodePatternId(0), NodePatternId(1), true, Symbol("bar")),
          EdgePattern(NodePatternId(2), NodePatternId(1), true, Symbol("foo"))
        ),
        NodePatternId(1),
        Seq(ReturnColumn.Id(NodePatternId(1), false, Symbol("id(m)"))),
        None,
        Nil,
        distinct = false
      )
    )

    testQuery(
      "MATCH (n)-[:bar]->(m)<-[:foo]-({ foo: \"BAR\" }) where m.name = [1,2] return id(m)",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            None,
            Map()
          ),
          NodePattern(
            NodePatternId(1),
            Set(),
            None,
            Map(
              Symbol("name") -> PropertyValuePattern.Value(
                QuineValue.List(
                  Vector(
                    QuineValue.Integer(1L),
                    QuineValue.Integer(2L)
                  )
                )
              )
            )
          ),
          NodePattern(
            NodePatternId(2),
            Set(),
            None,
            Map(Symbol("foo") -> PropertyValuePattern.Value(QuineValue.Str("BAR")))
          )
        ),
        List(
          EdgePattern(NodePatternId(0), NodePatternId(1), true, Symbol("bar")),
          EdgePattern(NodePatternId(2), NodePatternId(1), true, Symbol("foo"))
        ),
        NodePatternId(1),
        Seq(ReturnColumn.Id(NodePatternId(1), false, Symbol("id(m)"))),
        None,
        Nil,
        distinct = false
      )
    )

    testQuery(
      "MATCH (n) WHERE n.foo IS NOT NULL AND n.foo % 3 = 1 RETURN n.foo AS fooValue, n.foo*3 AS fooValueTripled",
      GraphQueryPattern(
        NonEmptyList.of(
          NodePattern(
            NodePatternId(0),
            Set(),
            None,
            Map(
              Symbol("foo") -> PropertyValuePattern.AnyValue
            )
          )
        ),
        Nil,
        NodePatternId(0),
        Seq(ReturnColumn.Property(NodePatternId(0), Symbol("foo"), Symbol("fooValue"))),
        Some(Expr.Equal(Expr.Modulo(Expr.Variable(Symbol("fooValue")), Expr.Integer(3L)), Expr.Integer(1L))),
        List(
          Symbol("fooValue") -> Expr.Variable(Symbol("fooValue")),
          Symbol("fooValueTripled") -> Expr.Multiply(Expr.Variable(Symbol("fooValue")), Expr.Integer(3L))
        ),
        distinct = false
      )
    )

  }

  describe("Error messages") {

    // should reject illegal MATCH -- TODO enable this test (QU-1292)
    {
      val query = "MATCH (n) WHERE exists((n)--()) RETURN id(n)"
      interceptQuery(query, CypherException.Compile("Illegal exists() clause in standing query", None), skip = true)
    }

    // should reject ORDER BY clause (something more than just `MATCH ... WHERE ... RETURN [DISTINCT]`)
    {
      val query = "MATCH (n) WHERE n.foo IS NOT NULL RETURN id(n) ORDER BY n.qux"
      interceptQuery(
        query,
        CypherException.Compile(
          "Wrong format for a standing query (expected `MATCH ... WHERE ... RETURN ...`)",
          Some(Position(1, 1, 0, SourceText(query)))
        )
      )
    }

    // should reject naming an edge
    {
      val query = "MATCH (n)-[e:Foo]->(m) RETURN id(n), e.type, id(m)"
      interceptQuery(
        query,
        CypherException.Compile(
          "Assigning edges to variables is not yet supported in standing query patterns",
          Some(Position(1, 10, 9, SourceText(query)))
        )
      )
    }

    // should reject giving more than one label to an edge
    {
      val query = "MATCH (n)-[:Foo|:Bar]->(m) RETURN id(n), id(m)"
      interceptQuery(
        query,
        CypherException.Compile(
          "Edges in standing query patterns must have exactly one label (got ColonDisjunction(Leaf(RelTypeName(Foo)),Leaf(RelTypeName(Bar))))",
          Some(Position(1, 10, 9, SourceText(query)))
        )
      )
    }

    // should reject undirected edge patterns
    {
      val query = "MATCH (n)-[:Foo]-(m) RETURN id(n), id(m)"
      interceptQuery(
        query,
        CypherException.Compile(
          "Edge in standing queries must specify a direction",
          Some(Position(1, 10, 9, SourceText(query)))
        )
      )
    }

    // should reject general use of a node variable
    {
      val query = "MATCH (n) WHERE size(keys(n)) > 2 RETURN id(n)"
      interceptQuery(
        query,
        CypherException.Compile(
          "Invalid use of node variable `n` (in standing queries, node variables can only reference constant properties or IDs)",
          Some(Position(1, 27, 26, SourceText(query)))
        )
      )
    }

    // should reject usage of an unbound variable
    {
      val query = "MATCH (n) RETURN m.foo"
      interceptQuery(
        query,
        CypherException.Compile(
          "Variable `m` not defined",
          Some(Position(1, 18, 17, SourceText(query)))
        )
      )
    }
  }
}
