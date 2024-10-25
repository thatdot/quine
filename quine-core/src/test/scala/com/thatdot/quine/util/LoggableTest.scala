package com.thatdot.quine.util

import java.time.LocalDate

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters._

import org.scalatest.funspec.AnyFunSpecLike

import com.thatdot.quine.graph.cypher.Expr
import com.thatdot.quine.graph.cypher.Func.UserDefined
import com.thatdot.quine.model.{QuineId, QuineValue}

import Log.implicits.{logExpr, logQuineValue, LogQuineIdRaw}

class LoggableTest extends AnyFunSpecLike {
  describe("cypher.Expr") {
    def asLog(expr: Expr): String = logExpr.unsafe(expr, _ => "*")
    def asSafeLog(expr: Expr): String = logExpr.safe(expr)
    def noWhitespace(str: String): String = str.replaceAll(raw"\s", "")

    it("formats a boolean") {
      withClue("without revealing its value") {
        assert(asLog(Expr.False) == asLog(Expr.True))
        assert(asLog(Expr.True) == """Bool(*)""")
      }
      withClue("revealing its value") {
        assert(asSafeLog(Expr.True) == """Bool(True)""")
        assert(asSafeLog(Expr.False) == """Bool(False)""")
      }
    }
    it("formats a map") {
      withClue("without revealing its value") {
        assert(asLog(Expr.Map()) == """Map()""")
        assert(asLog(Expr.Map("age" -> Expr.Integer(29))) == """Map(* -> Integer(*))""")
      }
      withClue("revealing its value") {
        assert(asSafeLog(Expr.Map("age" -> Expr.Integer(29))) == """Map(age -> Integer(29))""")
      }
    }
    it("formats a deep structure") {
      val structure = Expr.ListLiteral(
        Vector(
          Expr.Divide(Expr.Integer(1), Expr.Integer(0)),
          Expr.Str("This statement is false"),
          Expr.Case(
            Some(Expr.True),
            Vector(
              Expr.Null -> Expr.DynamicProperty(Expr.Duration(1.hours.toJava), Expr.Str("parsecs")),
              Expr.Str("true") -> Expr.Add(Expr.Str("antimatter"), Expr.Str("matter")),
            ),
            Some(Expr.FreshNodeId),
          ),
          Expr.Function(
            UserDefined("strId"),
            Vector(
              Expr.Node(
                QuineId(Array(0x12, 0x34)),
                labels = Set.empty,
                Map(Symbol("__LABEL") -> Expr.List(Expr.Str("jk"))),
              ),
            ),
          ),
        ),
      )

      withClue("revealing its value") {
        val unsanitized =
          """ListLiteral(
            |  Divide(Integer(1), Integer(0)),
            |  Str("This statement is false"),
            |  Case(
            |    Some(Bool(True)),
            |    {
            |      Null -> DynamicProperty(Duration(PT1H), Str("parsecs")),
            |      Str("true") -> Add(Str("antimatter"), Str("matter"))
            |    },
            |    Some(FreshNodeId)
            |  ),
            |  Function(
            |    strId,
            |    Arguments(
            |      Node(
            |        QuineId(1234),
            |        Labels(),
            |        {__LABEL -> List(Str("jk"))}
            |      )
            |    )
            |  )
            |)
            |""".stripMargin
        assert(noWhitespace(asSafeLog(structure)) == noWhitespace(unsanitized))
      }
      withClue("without revealing its value") {
        val sanitized =
          """ListLiteral(
            |  Divide(Integer(*), Integer(*)),
            |  Str(*),
            |  Case(
            |    Some(Bool(*)),
            |    {
            |      Null -> DynamicProperty(Duration(*), Str(*)),
            |      Str(*) -> Add(Str(*), Str(*))
            |    },
            |    Some(FreshNodeId)
            |  ),
            |  Function(
            |    strId,
            |    Arguments(
            |      Node(
            |        QuineId(1234),
            |        Labels(*),
            |        {* -> List(Str(*))}
            |      )
            |    )
            |  )
            |)
            |""".stripMargin
        assert(noWhitespace(asLog(structure)) == noWhitespace(sanitized))
      }
    }

  }

  describe("QuineValue") {
    def asLog(qv: QuineValue): String = logQuineValue.unsafe(qv, _ => "*")
    def asSafeLog(qv: QuineValue): String = logQuineValue.safe(qv)

    it("formats a Str") {
      withClue("without revealing its value") {
        assert(asLog(QuineValue.Str("hello")) == "Str(*)")
      }
      withClue("revealing its value") {
        assert(asSafeLog(QuineValue.Str("hello")) == """Str("hello")""")
      }
    }
    it("formats a List") {
      withClue("without revealing its value") {
        assert(
          asLog(
            QuineValue.List(Vector(QuineValue.Integer(100L), QuineValue.Str("world"))),
          ) == """List(Integer(*), Str(*))""",
        )
      }
      withClue("revealing its value") {
        assert(
          asSafeLog(
            QuineValue.List(Vector(QuineValue.Str("hello"), QuineValue.Str("world"))),
          ) == """List(Str("hello"), Str("world"))""",
        )
      }
    }
    it("formats a Map") {
      withClue("without revealing its value") {
        assert(
          asLog(
            QuineValue.Map(Map("hello" -> QuineValue.Null, "world" -> QuineValue.Date(LocalDate.EPOCH))),
          ) == "Map(* -> Null, * -> Date(*))",
        )
      }
      withClue("revealing its value") {
        assert(
          asSafeLog(
            QuineValue.Map(Map("hello" -> QuineValue.Null, "world" -> QuineValue.Date(LocalDate.EPOCH))),
          ) == "Map(hello -> Null, world -> Date(1970-01-01))",
        )
      }
    }
    it("makes indistinguishable same-shape values") {
      withClue("(lists of strings)") {
        assert(
          asLog(
            QuineValue.List(Vector(QuineValue.Str("a"), QuineValue.Str("b"))),
          ) == asLog(
            QuineValue.List(Vector(QuineValue.Str("x"), QuineValue.Str("y"))),
          ),
        )
      }
      withClue("(maps)") {
        assert(
          asLog(
            QuineValue.Map(Map("a" -> QuineValue.Str("b"), "c" -> QuineValue.Str("d"))),
          ) ==
            asLog(
              QuineValue.Map(Map("x" -> QuineValue.Str("y"), "z" -> QuineValue.Str("w"))),
            ),
        )
      }
    }
  }
}
