package com.thatdot.quine.app.ingest.serialization

import scala.util.Success

import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.cypher.Expr
import com.thatdot.quine.util.Log._

class ImportFormatTest extends AnyFunSuite {

  test("Json import properly deserializes large numbers") {

    val format = new CypherJsonInputFormat("MATCH (n) WHERE id(n) = 1 RETURN n", "n")(LogConfig.testing)
    def testInput(jsonString: String) = format.importBytes(jsonString.getBytes("UTF-8"))

    val l = Long.MaxValue - 1
    // Long.MaxValue-1 == 9223372036854775806 but returns 9223372036854775807 when rounded through a double:
    // i.e. l.doubleValue().longValue == 9223372036854775807
    // we don't use Long.MaxValue for this test since it doesn't change in this rounding
    assert(testInput(f"$l") == Success(Expr.Integer(l)))
    assert(testInput(f"[$l]") == Success(Expr.List(Expr.Integer(l))))
    assert(testInput(f"""{"a":$l}""") == Success(Expr.Map(Map("a" -> Expr.Integer(l)))))
  }
}
