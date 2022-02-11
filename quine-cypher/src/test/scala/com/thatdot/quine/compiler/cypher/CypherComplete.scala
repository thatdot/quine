package com.thatdot.quine.compiler.cypher

import scala.concurrent.{Await, ExecutionContext, Future}

import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.{
  CypherException,
  Expr,
  Type,
  UserDefinedFunction,
  UserDefinedFunctionSignature,
  UserDefinedProcedure,
  Value
}
import com.thatdot.quine.model.{QuineIdProvider, QuineValue}

class CypherComplete extends CypherHarness("cypher-complete-tests") {

  case class Person(
    first: String,
    last: String,
    birthYear: Option[Long],
    hasMother: Option[idProv.CustomIdType],
    hasFather: Option[idProv.CustomIdType],
    id: idProv.CustomIdType = idProv.newCustomId()
  )

  val people: List[Person] = {
    val ancestors = Person("Ancestors", "Ancestors", None, None, None)
    val arthur = Person("Arthur", "Weasley", Some(1950), Some(ancestors.id), Some(ancestors.id))
    val molly = Person("Molly", "Weasley", Some(1949), Some(ancestors.id), Some(ancestors.id))
    val ron = Person("Ron", "Weasley", Some(1980), Some(molly.id), Some(arthur.id))
    val mrs = Person("Missus", "Granger", None, Some(ancestors.id), Some(ancestors.id))
    val mr = Person("Mister", "Granger", None, Some(ancestors.id), Some(ancestors.id))
    val herm = Person("Hermione", "Granger", Some(1979), Some(mrs.id), Some(mr.id))
    val rose = Person("Rose", "Granger", Some(2005), Some(herm.id), Some(ron.id))
    val hugo = Person("Hugo", "Granger", Some(2008), Some(herm.id), Some(ron.id))
    List(ancestors, arthur, molly, ron, mrs, mr, herm, rose, hugo)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    import idProv.ImplicitConverters._

    Await.result(
      Future.traverse(people) { (person: Person) =>
        val mother = person.hasMother.getOrElse(person.id)
        val father = person.hasFather.getOrElse(person.id)
        for {
          _ <- graph.literalOps.setProp(person.id, "first", QuineValue.Str(person.first))
          _ <- graph.literalOps.setProp(person.id, "last", QuineValue.Str(person.last))
          _ <- person.birthYear.fold(Future.unit) { year =>
            graph.literalOps.setProp(person.id, "birthYear", QuineValue.Integer(year))
          }
          _ <- graph.literalOps.addEdge(person.id, mother, "has_mother")
          _ <- graph.literalOps.addEdge(person.id, father, "has_father")
        } yield ()
      },
      timeout.duration
    )

    ()
  }

  describe("`WITH` query clause") {
    testQuery(
      "WITH 1 + 2 AS x RETURN x",
      expectedColumns = Vector("x"),
      expectedRows = Seq(Vector(Expr.Integer(3L)))
    )

    testQuery(
      "WITH 1 + 2 AS x WHERE x > 2 RETURN x",
      expectedColumns = Vector("x"),
      expectedRows = Seq(Vector(Expr.Integer(3L)))
    )

    testQuery(
      "WITH 1 + 2 AS x WHERE x > 3 RETURN x",
      expectedColumns = Vector("x"),
      expectedRows = Seq()
    )
  }

  describe("`UNION` query clause") {
    testQuery(
      "WITH 3 AS x RETURN x UNION WITH \"str\" as x RETURN x",
      expectedColumns = Vector("x"),
      expectedRows = Seq(Vector(Expr.Integer(3L)), Vector(Expr.Str("str"))),
      ordered = false
    )
  }

  describe("`UNWIND` query clause") {
    testQuery(
      "UNWIND [1,2,3,1,2] AS x RETURN x",
      expectedColumns = Vector("x"),
      expectedRows = Seq(
        Vector(Expr.Integer(1L)),
        Vector(Expr.Integer(2L)),
        Vector(Expr.Integer(3L)),
        Vector(Expr.Integer(1L)),
        Vector(Expr.Integer(2L))
      )
    )

    testQuery(
      "UNWIND [1, 2, 3, NULL ] AS x RETURN x, 'val' AS y",
      expectedColumns = Vector("x", "y"),
      expectedRows = Seq(
        Vector(Expr.Integer(1L), Expr.Str("val")),
        Vector(Expr.Integer(2L), Expr.Str("val")),
        Vector(Expr.Integer(3L), Expr.Str("val")),
        Vector(Expr.Null, Expr.Str("val"))
      )
    )

    testQuery(
      """WITH [1, 1, 2, 2] AS coll
        |UNWIND coll AS x
        |RETURN collect(DISTINCT x) AS setOfVals""".stripMargin('|'),
      expectedColumns = Vector("setOfVals"),
      expectedRows = Seq(Vector(Expr.List(Vector(Expr.Integer(1L), Expr.Integer(2L)))))
    )

    testQuery(
      "WITH [1, 2] AS a,[3, 4] AS b UNWIND (a + b) AS x RETURN x",
      expectedColumns = Vector("x"),
      expectedRows = Seq(
        Vector(Expr.Integer(1L)),
        Vector(Expr.Integer(2L)),
        Vector(Expr.Integer(3L)),
        Vector(Expr.Integer(4L))
      )
    )

    testQuery(
      """WITH [[1, 2],[3, 4], 5] AS nested
        |UNWIND nested AS x
        |UNWIND x AS y
        |RETURN y""".stripMargin('|'),
      expectedColumns = Vector("y"),
      expectedRows = Seq(
        Vector(Expr.Integer(1L)),
        Vector(Expr.Integer(2L)),
        Vector(Expr.Integer(3L)),
        Vector(Expr.Integer(4L)),
        Vector(Expr.Integer(5L))
      )
    )

    testQuery(
      "UNWIND [] AS empty RETURN empty, 'literal_that_is_not_returned'",
      expectedColumns = Vector("empty", "'literal_that_is_not_returned'"),
      expectedRows = Seq.empty
    )

    testQuery(
      "UNWIND NULL AS x RETURN x, 'some_literal'",
      expectedColumns = Vector("x", "'some_literal'"),
      expectedRows = Seq.empty
    )

    testQuery(
      "UNWIND [1,2,3,4,56] AS nId MATCH (n) WHERE ID(n) = nId RETURN n.prop",
      expectedColumns = Vector("n.prop"),
      expectedRows = Seq.fill(5)(Vector(Expr.Null))
    )
  }

  describe("`MATCH` query clause") {
    testQuery(
      "MATCH (p)-[:has_mother]->(m) RETURN p.first, m.first",
      expectedColumns = Vector("p.first", "m.first"),
      expectedRows = Seq(
        Vector(Expr.Str("Mister"), Expr.Str("Ancestors")),
        Vector(Expr.Str("Molly"), Expr.Str("Ancestors")),
        Vector(Expr.Str("Hermione"), Expr.Str("Missus")),
        Vector(Expr.Str("Ron"), Expr.Str("Molly")),
        Vector(Expr.Str("Rose"), Expr.Str("Hermione")),
        Vector(Expr.Str("Hugo"), Expr.Str("Hermione")),
        Vector(Expr.Str("Ancestors"), Expr.Str("Ancestors")),
        Vector(Expr.Str("Missus"), Expr.Str("Ancestors")),
        Vector(Expr.Str("Arthur"), Expr.Str("Ancestors"))
      ),
      expectedCanContainAllNodeScan = true,
      ordered = false
    )

    testQuery(
      "MATCH (p { last: \"Granger\" }) RETURN p.first",
      expectedColumns = Vector("p.first"),
      expectedRows = Seq(
        Vector(Expr.Str("Mister")),
        Vector(Expr.Str("Hermione")),
        Vector(Expr.Str("Rose")),
        Vector(Expr.Str("Hugo")),
        Vector(Expr.Str("Missus"))
      ),
      expectedCanContainAllNodeScan = true,
      ordered = false
    )

    describe("Pattern structure is normalized") {
      testQuery(
        "MATCH (c)-[:has_mother]->(m)-[:has_mother]->(a)<-[:has_father]-(f)<-[:has_father]-(c) RETURN c.first, a.first",
        expectedColumns = Vector("c.first", "a.first"),
        expectedRows = Seq(
          Vector(Expr.Str("Molly"), Expr.Str("Ancestors")),
          Vector(Expr.Str("Ron"), Expr.Str("Ancestors")),
          Vector(Expr.Str("Mister"), Expr.Str("Ancestors")),
          Vector(Expr.Str("Hermione"), Expr.Str("Ancestors")),
          Vector(Expr.Str("Missus"), Expr.Str("Ancestors")),
          Vector(Expr.Str("Arthur"), Expr.Str("Ancestors"))
        ),
        expectedCanContainAllNodeScan = true,
        ordered = false
      )

      testQuery(
        "MATCH (c)-[:has_mother]->(m)-[:has_mother]->(a), (c)-[:has_father]->(f)-[:has_father]->(a) RETURN c.first, a.first",
        expectedColumns = Vector("c.first", "a.first"),
        expectedRows = Seq(
          Vector(Expr.Str("Molly"), Expr.Str("Ancestors")),
          Vector(Expr.Str("Ron"), Expr.Str("Ancestors")),
          Vector(Expr.Str("Mister"), Expr.Str("Ancestors")),
          Vector(Expr.Str("Hermione"), Expr.Str("Ancestors")),
          Vector(Expr.Str("Missus"), Expr.Str("Ancestors")),
          Vector(Expr.Str("Arthur"), Expr.Str("Ancestors"))
        ),
        expectedCanContainAllNodeScan = true,
        ordered = false
      )

      testQuery(
        "MATCH (c)-[:has_mother]->(m), (m)-[:has_mother]->(a), (c)-[:has_father]->(f)-[:has_father]->(a) RETURN c.first, a.first",
        expectedColumns = Vector("c.first", "a.first"),
        expectedRows = Seq(
          Vector(Expr.Str("Molly"), Expr.Str("Ancestors")),
          Vector(Expr.Str("Ron"), Expr.Str("Ancestors")),
          Vector(Expr.Str("Mister"), Expr.Str("Ancestors")),
          Vector(Expr.Str("Hermione"), Expr.Str("Ancestors")),
          Vector(Expr.Str("Missus"), Expr.Str("Ancestors")),
          Vector(Expr.Str("Arthur"), Expr.Str("Ancestors"))
        ),
        expectedCanContainAllNodeScan = true,
        ordered = false
      )
    }
  }

  describe("`ORDER BY`, `SKIP`, and `LIMIT` query clauses") {
    testQuery(
      "MATCH (p)-[:has_mother]->(m) RETURN p.first, m.first ORDER BY p.first",
      expectedColumns = Vector("p.first", "m.first"),
      expectedRows = Seq(
        Vector(Expr.Str("Ancestors"), Expr.Str("Ancestors")),
        Vector(Expr.Str("Arthur"), Expr.Str("Ancestors")),
        Vector(Expr.Str("Hermione"), Expr.Str("Missus")),
        Vector(Expr.Str("Hugo"), Expr.Str("Hermione")),
        Vector(Expr.Str("Missus"), Expr.Str("Ancestors")),
        Vector(Expr.Str("Mister"), Expr.Str("Ancestors")),
        Vector(Expr.Str("Molly"), Expr.Str("Ancestors")),
        Vector(Expr.Str("Ron"), Expr.Str("Molly")),
        Vector(Expr.Str("Rose"), Expr.Str("Hermione"))
      ),
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (p)-[:has_mother]->(m) RETURN p.last, m.first ORDER BY p.first SKIP 1 LIMIT 3",
      expectedColumns = Vector("p.last", "m.first"),
      expectedRows = Seq(
        Vector(Expr.Str("Weasley"), Expr.Str("Ancestors")),
        Vector(Expr.Str("Granger"), Expr.Str("Missus")),
        Vector(Expr.Str("Granger"), Expr.Str("Hermione"))
      ),
      expectedCanContainAllNodeScan = true
    )

    testQuery(
      "MATCH (p)-[:has_mother]->(m) RETURN p.first, m.first ORDER BY p.last DESC, p.first LIMIT 2",
      expectedColumns = Vector("p.first", "m.first"),
      expectedRows = Seq(
        Vector(Expr.Str("Arthur"), Expr.Str("Ancestors")),
        Vector(Expr.Str("Molly"), Expr.Str("Ancestors"))
      ),
      expectedCanContainAllNodeScan = true
    )
  }

  describe("edges should be ordered") {
    testQuery(
      "MATCH (c)-->(p) WHERE c.first = 'Rose' RETURN p.first",
      expectedColumns = Vector("p.first"),
      expectedRows = Seq(
        Vector(Expr.Str("Hermione")),
        Vector(Expr.Str("Ron"))
      ),
      expectedCanContainAllNodeScan = true
    )
  }

  describe("`RETURN` query clause") {

    testQuery(
      "RETURN 1 + 2 AS num1, \"hello\" + \"!\"",
      expectedColumns = Vector("num1", "\"hello\" + \"!\""),
      expectedRows = Seq(Vector(Expr.Integer(3L), Expr.Str("hello!")))
    )

    testQuery(
      "RETURN 1 AS k, 2 AS b, 3 AS d, 4 AS e, 5 AS x, 6 AS q, 7 AS o, 8 AS l",
      expectedColumns = Vector("k", "b", "d", "e", "x", "q", "o", "l"),
      expectedRows = Seq((1 to 8).map(i => Expr.Integer(i.toLong)).toVector)
    )

    testQuery(
      "UNWIND [['a', 'b'], ['c']] as x UNWIND [1, 2, 3] as y UNWIND x as xs return y, xs",
      expectedColumns = Vector("y", "xs"),
      expectedRows = Seq(
        Vector(Expr.Integer(1L), Expr.Str("a")),
        Vector(Expr.Integer(1L), Expr.Str("b")),
        Vector(Expr.Integer(2L), Expr.Str("a")),
        Vector(Expr.Integer(2L), Expr.Str("b")),
        Vector(Expr.Integer(3L), Expr.Str("a")),
        Vector(Expr.Integer(3L), Expr.Str("b")),
        Vector(Expr.Integer(1L), Expr.Str("c")),
        Vector(Expr.Integer(2L), Expr.Str("c")),
        Vector(Expr.Integer(3L), Expr.Str("c"))
      )
    )

    describe("aggregations") {
      testQuery(
        "RETURN 1 AS k, count(*) AS b, 3 AS d, 4 AS e, collect(5) AS x, 6 AS q, sum(7) AS o, 8 AS l",
        expectedColumns = Vector("k", "b", "d", "e", "x", "q", "o", "l"),
        expectedRows = Seq(
          Vector(
            Expr.Integer(1L),
            Expr.Integer(1L),
            Expr.Integer(3L),
            Expr.Integer(4L),
            Expr.List(Vector(Expr.Integer(5L))),
            Expr.Integer(6L),
            Expr.Integer(7L),
            Expr.Integer(8L)
          )
        )
      )

      testQuery(
        "UNWIND [1,1,2] AS x UNWIND [4,5,6] AS y RETURN count(x), y",
        expectedColumns = Vector("count(x)", "y"),
        expectedRows = Seq(
          Vector(Expr.Integer(3L), Expr.Integer(5L)),
          Vector(Expr.Integer(3L), Expr.Integer(6L)),
          Vector(Expr.Integer(3L), Expr.Integer(4L))
        )
      )

      testQuery(
        "MATCH (p)-[:has_mother]->(m) RETURN count(*)",
        expectedColumns = Vector("count(*)"),
        expectedRows = Seq(Vector(Expr.Integer(9L))),
        expectedCanContainAllNodeScan = true
      )

      testQuery(
        "MATCH (p)-[:has_mother]->(m) RETURN m.first, count(p)",
        expectedColumns = Vector("m.first", "count(p)"),
        expectedRows = Seq(
          Vector(Expr.Str("Ancestors"), Expr.Integer(5L)),
          Vector(Expr.Str("Missus"), Expr.Integer(1L)),
          Vector(Expr.Str("Molly"), Expr.Integer(1L)),
          Vector(Expr.Str("Hermione"), Expr.Integer(2L))
        ),
        expectedCanContainAllNodeScan = true,
        ordered = false
      )

      testQuery(
        "MATCH (p)-[:has_mother]->(m) WITH m, p ORDER BY p.first RETURN m.first, collect(p.first)",
        expectedColumns = Vector("m.first", "collect(p.first)"),
        expectedRows = Seq(
          Vector(
            Expr.Str("Ancestors"),
            Expr.List(
              Vector(
                Expr.Str("Ancestors"),
                Expr.Str("Arthur"),
                Expr.Str("Missus"),
                Expr.Str("Mister"),
                Expr.Str("Molly")
              )
            )
          ),
          Vector(
            Expr.Str("Hermione"),
            Expr.List(Vector(Expr.Str("Hugo"), Expr.Str("Rose")))
          ),
          Vector(
            Expr.Str("Molly"),
            Expr.List(Vector(Expr.Str("Ron")))
          ),
          Vector(
            Expr.Str("Missus"),
            Expr.List(Vector(Expr.Str("Hermione")))
          )
        ),
        expectedCanContainAllNodeScan = true,
        ordered = false
      )

      testQuery(
        "UNWIND [1,2,3,4,5] AS x RETURN 4 * count(*) + collect(x ^ 2) AS N",
        expectedColumns = Vector("N"),
        expectedRows = Seq(
          Vector(
            Expr.List(
              Vector(
                Expr.Integer(20),
                Expr.Floating(1.0),
                Expr.Floating(4.0),
                Expr.Floating(9.0),
                Expr.Floating(16.0),
                Expr.Floating(25.0)
              )
            )
          )
        )
      )
    }

    describe("`DISTINCT` at the top level and in aggregations") {
      testQuery(
        "UNWIND [1,2,3,4,5] AS x RETURN x = 3",
        expectedColumns = Vector("x = 3"),
        expectedRows = Seq(
          Vector(Expr.False),
          Vector(Expr.False),
          Vector(Expr.True),
          Vector(Expr.False),
          Vector(Expr.False)
        )
      )

      testQuery(
        "UNWIND [1,2,3,4,5] AS x RETURN DISTINCT x = 3",
        expectedColumns = Vector("x = 3"),
        expectedRows = Seq(
          Vector(Expr.False),
          Vector(Expr.True)
        )
      )

      testQuery(
        "UNWIND [1,2,3,1,2] AS x RETURN count(x)",
        expectedColumns = Vector("count(x)"),
        expectedRows = Seq(Vector(Expr.Integer(5L)))
      )

      testQuery(
        "UNWIND [1,2,3,1,2] AS x RETURN count(DISTINCT x)",
        expectedColumns = Vector("count(DISTINCT x)"),
        expectedRows = Seq(Vector(Expr.Integer(3L)))
      )

      testQuery(
        "UNWIND [1,2,3,1,2] AS x RETURN DISTINCT x",
        expectedColumns = Vector("x"),
        expectedRows = Seq(
          Vector(Expr.Integer(1L)),
          Vector(Expr.Integer(2L)),
          Vector(Expr.Integer(3L))
        )
      )

      testQuery(
        "UNWIND [1,2,3,1,2] AS x RETURN DISTINCT count(x)",
        expectedColumns = Vector("count(x)"),
        expectedRows = Seq(Vector(Expr.Integer(5L)))
      )

      testQuery(
        "UNWIND [1,2,3,1,2] AS x RETURN DISTINCT count(DISTINCT x)",
        expectedColumns = Vector("count(DISTINCT x)"),
        expectedRows = Seq(Vector(Expr.Integer(3L)))
      )
    }
  }

  describe("User defined functions") {
    registerUserDefinedFunction(MyReverse)
    testQuery(
      "RETURN myreverse(\"hello\") AS REV",
      expectedColumns = Vector("REV"),
      expectedRows = Seq(Vector(Expr.Str("olleh")))
    )
  }

  describe("`CALL` query clause for user defined procedures") {
    registerUserDefinedProcedure(MyUnwind)
    testQuery(
      "CALL myunwind([1,2,\"hello\",null])",
      expectedColumns = Vector("unwound"),
      expectedRows = Seq(
        Vector(Expr.Integer(1L)),
        Vector(Expr.Integer(2L)),
        Vector(Expr.Str("hello")),
        Vector(Expr.Null)
      )
    )

    testQuery(
      """CALL myunwind([2,3,1,4,7])
        |YIELD unwound AS x
        |WHERE x > 2
        |RETURN x""".stripMargin('|'),
      expectedColumns = Vector("x"),
      expectedRows = Seq(
        Vector(Expr.Integer(3L)),
        Vector(Expr.Integer(4L)),
        Vector(Expr.Integer(7L))
      )
    )

    testQuery(
      "CALL myunwind",
      parameters = Map("list" -> Expr.List(Expr.Str("hi"), Expr.Str("world"))),
      expectedColumns = Vector("unwound"),
      expectedRows = Seq(
        Vector(Expr.Str("hi")),
        Vector(Expr.Str("world"))
      )
    )
  }

  describe("Functions and procedures are case insensitive") {
    testQuery(
      "RETURN hEaD(['heLLo'])",
      expectedColumns = Vector("hEaD(['heLLo'])"),
      expectedRows = Seq(Vector(Expr.Str("heLLo")))
    )

    testQuery(
      "CALL mYuNwINd(['heLLo'])",
      expectedColumns = Vector("unwound"),
      expectedRows = Seq(Vector(Expr.Str("heLLo")))
    )
  }

  testQuery(
    """MATCH (person)-[:has_mother]->(mom)
      |OPTIONAL MATCH (sibling)-[:has_mother]->(mom)
      |WHERE person <> sibling AND mom.first <> "Ancestors"
      |RETURN person.first, sibling.first""".stripMargin('|'),
    expectedColumns = Vector("person.first", "sibling.first"),
    expectedRows = Seq(
      Vector(Expr.Str("Hermione"), Expr.Null),
      Vector(Expr.Str("Rose"), Expr.Str("Hugo")),
      Vector(Expr.Str("Molly"), Expr.Null),
      Vector(Expr.Str("Arthur"), Expr.Null),
      Vector(Expr.Str("Missus"), Expr.Null),
      Vector(Expr.Str("Ancestors"), Expr.Null),
      Vector(Expr.Str("Ron"), Expr.Null),
      Vector(Expr.Str("Hugo"), Expr.Str("Rose")),
      Vector(Expr.Str("Mister"), Expr.Null)
    ),
    expectedCanContainAllNodeScan = true,
    ordered = false
  )

  describe("`FOREACH`, `SET`, `REMOVE` should always return exactly one row") {

    /* `OPTIONAL MATCH (n) WHERE id(n) = null` ensures we have `n` of type node
     * in context, but it will be null and all of the `SET`/`REMOVE`/`DELETE`
     * won't have anything to do. They should still return exactly 1 row.
     */
    testQuery(
      "OPTIONAL MATCH (n) WHERE id(n) = null SET n.foo = 1 RETURN 1",
      expectedColumns = Vector("1"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedIsReadOnly = false
    )
    testQuery(
      "OPTIONAL MATCH (n) WHERE id(n) = null REMOVE n.foo RETURN 1",
      expectedColumns = Vector("1"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedIsReadOnly = false
    )
    testQuery(
      "OPTIONAL MATCH (n) WHERE id(n) = null DELETE n RETURN 1",
      expectedColumns = Vector("1"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedIsReadOnly = false
    )

    testQuery(
      "OPTIONAL MATCH (n) WHERE id(n) = null FOREACH (x IN [] | DELETE n) RETURN 1",
      expectedColumns = Vector("1"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedIsReadOnly = false
    )
    testQuery(
      "OPTIONAL MATCH (n) WHERE id(n) = null FOREACH (x IN [1,2,3] | DELETE n) RETURN 1",
      expectedColumns = Vector("1"),
      expectedRows = Seq(Vector(Expr.Integer(1L))),
      expectedIsReadOnly = false
    )
  }

  describe("Exceptions") {
    describe("TypeMismatch") {
      interceptQuery(
        "MATCH (p) WHERE p.first = 'Molly' RETURN p.last / 1",
        CypherException.TypeMismatch(
          expected = Seq(Type.Number),
          actualValue = Expr.Str("Weasley"),
          context = "division"
        )
      )

      interceptQuery(
        "MATCH (p) WHERE p.first = 'Molly' RETURN p.last.nonExistantProperty",
        CypherException.TypeMismatch(
          expected = Seq(
            Type.Map,
            Type.Node,
            Type.Relationship,
            Type.LocalDateTime,
            Type.DateTime,
            Type.Duration
          ),
          actualValue = Expr.Str("Weasley"),
          context = "property access"
        )
      )
    }

    describe("Arithmetic") {
      interceptQuery(
        "MATCH (p) WHERE p.first = 'Molly' RETURN p.birthYear / 0",
        CypherException.Arithmetic(
          wrapping = "/ by zero",
          operands = Seq(Expr.Integer(1949L), Expr.Integer(0L))
        )
      )

      interceptQuery(
        "MATCH (p) WHERE p.first = 'Molly' WITH p.birthYear + 9223372036854775800 AS N RETURN 1",
        CypherException.Arithmetic(
          wrapping = "long overflow",
          operands = Seq(Expr.Integer(1949L), Expr.Integer(9223372036854775800L))
        )
      )
    }
  }
}

// For testing only...
object MyReverse extends UserDefinedFunction {

  val name = "myreverse"

  val isPure = true

  val signatures: Vector[UserDefinedFunctionSignature] = Vector(
    UserDefinedFunctionSignature(
      arguments = Vector("input" -> Type.Str),
      output = Type.Str,
      description = "Returns the string reversed"
    ),
    UserDefinedFunctionSignature(
      arguments = Vector("input" -> Type.ListOfAnything),
      output = Type.ListOfAnything,
      description = "Returns the list reversed"
    )
  )

  def call(args: Vector[Value])(implicit idp: QuineIdProvider): Value =
    args match {
      case Vector(Expr.Str(str)) => Expr.Str(str.reverse)
      case Vector(Expr.List(lst)) => Expr.List(lst.reverse)
    }
}
object MyUnwind extends UserDefinedProcedure {
  val name = "myunwind"
  val canContainUpdates = false
  val isIdempotent = true
  val canContainAllNodeScan = false
  val signature: cypher.UserDefinedProcedureSignature = cypher.UserDefinedProcedureSignature(
    arguments = Vector("list" -> cypher.Type.ListOfAnything),
    outputs = Vector("unwound" -> cypher.Type.Anything),
    description = "Unwind list"
  )

  def call(
    context: cypher.QueryContext,
    arguments: Seq[cypher.Value],
    location: cypher.ProcedureExecutionLocation
  )(implicit
    ec: ExecutionContext,
    parameters: cypher.Parameters,
    timeout: akka.util.Timeout
  ): akka.stream.scaladsl.Source[Vector[cypher.Value], _] =
    arguments match {
      case Seq(Expr.List(l)) => akka.stream.scaladsl.Source(l.map(Vector(_)))
      case _ => throw wrongSignature(arguments)
    }
}
