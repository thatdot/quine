package com.thatdot.quine.ingest2.transformation

import io.circe.Json
import org.graalvm.polyglot
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import com.thatdot.quine.app.model.transformation.polyglot.Polyglot
import com.thatdot.quine.graph.cypher

class DataFoldableSpec extends AnyFunSuite with ScalaCheckPropertyChecks with FoldableArbitraryHelpers {
  // Round trip tests for everything that it ended up reasonable to do for.
  // A round trip test takes a value from one representation converts it to another and translates
  // back comparing the result after those two transformations.
  // This only works for conversions out of the source that are invertible, e.g. cypher to json is not
  test("roundtrip Cypher") {
    forAll { (input: Json) =>
      val out: cypher.Value = convert[Json, cypher.Value](input)
      val roundtrip: Json = convert[cypher.Value, Json](out)
      assert(input == roundtrip)
    }
  }

  test("roundtrip Polyglot From Json") {
    forAll(arbJson.arbitrary) { (input: Json) =>
      val out = convert[Json, Polyglot.HostValue](input)
      val roundtrip = convert[polyglot.Value, Json](polyglot.Value.asValue(out))
      assert(input == roundtrip)
    }
  }

  test("roundtrip Polyglot From Cypher") {
    forAll(arbCypher.arbitrary) { (input: cypher.Value) =>
      val out = convert[cypher.Value, Polyglot.HostValue](input)
      val roundtrip = convert[polyglot.Value, cypher.Value](polyglot.Value.asValue(out))
      assert(input == roundtrip)
    }
  }

  test("roundtrip Cypher From Polygot") {
    forAll(arbPolyglot.arbitrary) { (input: polyglot.Value) =>
      val out = convert[polyglot.Value, cypher.Value](input)
      val roundtrip = polyglot.Value.asValue(convert[cypher.Value, Polyglot.HostValue](out))
      assert(input.toString == roundtrip.toString)
    }
  }

  test("roundtrip Polyglot From Cypher floating") {
    val input = cypher.Expr.Floating(1.9365476157539434e17)
    val out = convert[cypher.Value, Polyglot.HostValue](input)
    val roundtrip = convert[polyglot.Value, cypher.Value](polyglot.Value.asValue(out))
    assert(input == roundtrip)
  }

  test("floating branch round‑trips non‑integral doubles") {
    forAll(Gen.chooseNum(-1e6, 1e6).filter(d => !d.isWhole)) { d =>
      val j = cypher.Expr.Floating(d)
      val j2: cypher.Value =
        convert[polyglot.Value, cypher.Value](polyglot.Value.asValue(convert[cypher.Value, Polyglot.HostValue](j)))
      assert(j2 == j)
    }
  }

  test("floating branch round‑trips doubles") {
    forAll(Gen.chooseNum(Double.MinValue, Double.MaxValue)) { d =>
      val j = cypher.Expr.Floating(d)
      val j2: cypher.Value =
        convert[polyglot.Value, cypher.Value](polyglot.Value.asValue(convert[cypher.Value, Polyglot.HostValue](j)))
      assert(j2 == j)
    }
  }

  test("integer branch round‑trips doubles") {
    forAll(Gen.chooseNum(Long.MinValue, Long.MaxValue)) { d =>
      val j = cypher.Expr.Integer(d)
      val j2: cypher.Value =
        convert[polyglot.Value, cypher.Value](polyglot.Value.asValue(convert[cypher.Value, Polyglot.HostValue](j)))
      assert(j2 == j)
    }

  }

}
