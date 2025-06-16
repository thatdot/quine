package com.thatdot.quine.ingest2.transformation
import io.circe.Json
import org.graalvm.polyglot
import org.scalacheck.Gen
import org.scalatest.EitherValues._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import com.thatdot.data.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.app.model.transformation.polyglot.langauges.JavaScriptTransformation
import com.thatdot.quine.app.model.transformation.polyglot.{
  Polyglot,
  PolyglotValueDataFoldableFrom,
  PolyglotValueDataFolderTo,
}

class QuineJavaScriptSpec extends AnyFunSuite with ScalaCheckPropertyChecks with FoldableArbitraryHelpers {

  private def host(json: Json): Polyglot.HostValue =
    DataFoldableFrom[Json].fold(json, PolyglotValueDataFolderTo)

  private def json(v: polyglot.Value): Json =
    PolyglotValueDataFoldableFrom.fold(v, DataFolderTo.jsonFolder)

  test("Produce Object") {
    val tr = JavaScriptTransformation.makeInstance("that => ({num: that.values[0]})").value
    val jsonInput = Json.obj("values" -> Json.arr(Json.fromInt(0), Json.fromInt(1)))
    val inputValue = host(jsonInput)
    val out = tr(inputValue).value
    assert(out.getMember("num").asInt() == 0)
  }

  test("boolean inversion") {
    val tr = JavaScriptTransformation.makeInstance("(b) => !b").value
    val out = tr(host(Json.True)).value
    assert(out.isBoolean && !out.asBoolean())
  }

  test("integer multiply") {
    val tr = JavaScriptTransformation.makeInstance("(x) => x * 10").value
    val out = tr(host(Json.fromInt(7))).value
    assert(out.fitsInLong && out.asLong() == 70L)
  }

  test("floating‑point addition") {
    val tr = JavaScriptTransformation.makeInstance("(x) => x + 0.5").value
    val out = tr(host(Json.fromDoubleOrString(3.0))).value
    assert(out.fitsInDouble && math.abs(out.asDouble() - 3.5) < 1e-9)
  }

  test("string concatenation") {
    val tr = JavaScriptTransformation.makeInstance("(s) => s + ' world'").value
    val out = tr(host(Json.fromString("hello"))).value
    assert(out.isString && out.asString() == "hello world")
  }

  test("array of objects – vector builder & map builder together") {
    val fn = "(xs) => xs.map(x => ({ orig: x, double: x * 2 }))"
    val tr = JavaScriptTransformation.makeInstance(fn).value

    val inArray = Json.arr(Json.fromInt(1), Json.fromInt(2), Json.fromInt(3))
    val out = tr(host(inArray)).value

    assert(out.hasArrayElements)
    val triples = (0L until out.getArraySize).map { i =>
      val obj = out.getArrayElement(i)
      (obj.getMember("orig").asInt(), obj.getMember("double").asInt())
    }.toList
    assert(triples == List((1, 2), (2, 4), (3, 6)))
  }

  test("numeric increment function") {
    val tr = JavaScriptTransformation.makeInstance("(x) => x + 1").value
    val out = tr(host(Json.fromInt(5))).value
    assert(out.isNumber && out.asInt() == 6)
    assert(json(out).asNumber.flatMap(_.toInt) contains 6)
  }

  test("array map – increment each element") {
    val tr = JavaScriptTransformation.makeInstance("(arr) => arr.map(x => x + 1)").value
    val in = host(Json.arr(Json.fromInt(1), Json.fromInt(2)))
    val out = tr(in).value

    assert(out.hasArrayElements)
    val ints = (0L until out.getArraySize).map(i => out.getArrayElement(i).asInt()).toList
    assert(ints == List(2, 3))
  }

  test("object manipulation – double field value and JSON round‑trip") {
    val tr = JavaScriptTransformation.makeInstance("(o) => ({ foo: o.bar * 2 })").value
    val in = host(Json.obj("bar" -> Json.fromInt(10)))
    val out = tr(in).value

    val outJson = json(out)
    assert(outJson.hcursor.get[Int]("foo").value == 20)
  }

  test("handle nested JSON: pick, transform, and return object") {
    val fn =
      """(obj) => ({ result: obj.nested.values.reduce((a, b) => a + b, 0) })"""
    val tr = JavaScriptTransformation.makeInstance(fn).value

    val nested = Json.obj(
      "nested" -> Json.obj(
        "values" -> Json.arr(Json.fromInt(1), Json.fromInt(2), Json.fromInt(3)),
      ),
    )

    val out = tr(host(nested)).value
    assert(json(out).hcursor.get[Int]("result").value == 6)
  }

  private val identityTransform =
    JavaScriptTransformation.makeInstance("(x) => x").value

  test("identity JavaScript round‑trip arbitrary JSON") {
    forAll { (j: Json) =>
      val resultEither = identityTransform(host(j))
      assert(resultEither.isRight)
      val j2 = json(resultEither.value)
      assert(j2 == j)
    }
  }

  private val doubleArray =
    JavaScriptTransformation.makeInstance("(arr) => arr.map(x => x * 2)").value

  test("array doubling preserves length and doubles each element") {
    forAll(genIntArray) { arrJson: Json =>
      val doubledEither = doubleArray(host(arrJson))
      assert(doubledEither.isRight)

      val doubled = doubledEither.value
      assert(doubled.hasArrayElements && doubled.getArraySize == arrJson.asArray.get.size.toLong)

      val originalInts = arrJson.asArray.get.map(_.asNumber.get.toInt.get)
      val doubledInts = (0L until doubled.getArraySize).map(i => doubled.getArrayElement(i).asInt())
      assert(doubledInts == originalInts.map(_ * 2))
    }
  }

  test("identity round‑trip non‑empty arrays exactly") {
    forAll(genIntArray) { arrJson: Json =>
      val res = identityTransform(host(arrJson)).value
      val backJson = json(res)
      assert(backJson == arrJson)
    }
  }

  private val toUint8Array =
    JavaScriptTransformation.makeInstance("(xs) => new Uint8Array(xs)").value

  test("Uint8Array round‑trip bytes") {
    forAll(genBytes) { bytes: Vector[Byte] =>
      val jsonArr = Json.arr(bytes.map(b => Json.fromLong(b & 0xFFL)): _*)
      val result = json(toUint8Array(host(jsonArr)).value)
      assert(jsonArr == result)
    }
  }

  private val invertBool =
    JavaScriptTransformation.makeInstance("(b) => !b").value

  test("boolean inversion round‑trip") {
    forAll(Gen.oneOf(true, false)) { b: Boolean =>
      val jsonBool = if (b) Json.True else Json.False
      val res = invertBool(host(jsonBool)).value
      assert(res.isBoolean && res.asBoolean() == !b)
    }
  }

  private val toUint8 = JavaScriptTransformation.makeInstance("(xs) => new Uint8Array(xs)").value

  test("bytes branch through Uint8Array round‑trips") {
    forAll(Gen.nonEmptyListOf(Gen.choose(0, 255).map(_.toByte))) { bytesList =>
      val jsonArr = Json.arr(bytesList.map(b => Json.fromLong(b & 0xFFL)): _*)
      val out = toUint8(host(jsonArr)).value // JS produces Uint8Array
      val j2: Json = json(out)
      assert(j2 == jsonArr)
    }
  }

  test("integer shifting with delta") {
    val genPair = for {
      n <- Gen.chooseNum(-1000000L, 1000000L)
      d <- Gen.chooseNum(-1000L, 1000L)
    } yield (n, d)

    forAll(genPair) { case (n, d) =>
      val tr = JavaScriptTransformation.makeInstance(s"(x) => x + $d").value
      val res = tr(host(Json.fromLong(n))).value
      assert(res.fitsInLong && res.asLong() == n + d)
    }
  }

  private val concatSuffix = "_suffix"
  private val concatFn =
    JavaScriptTransformation.makeInstance(s"(s) => s + '$concatSuffix'").value

  test("string concatenation adds suffix") {
    forAll(Gen.alphaStr) { s: String =>
      val out = concatFn(host(Json.fromString(s))).value
      assert(out.isString && out.asString() == s + concatSuffix)
    }
  }

  private val doubleFields =
    JavaScriptTransformation.makeInstance("(o) => Object.fromEntries(Object.entries(o).map(([k,v]) => [k, v*2]))").value

  private val genNumObject: Gen[Json] =
    Gen
      .nonEmptyMap(
        Gen.zip(Gen.identifier, Gen.chooseNum(-1000, 1000).map(Json.fromInt)),
      )
      .map(fields => Json.obj(fields.toSeq: _*))

  test("object field values doubled") {
    forAll(genNumObject) { objJson: Json =>
      val out = doubleFields(host(objJson)).value
      val outJs = json(out)

      val inVals = objJson.asObject.get.values.map(_.asNumber.get.toInt.get)
      val outVals = outJs.asObject.get.values.map(_.asNumber.get.toInt.get)
      assert(outVals.toSet == inVals.map(_ * 2).toSet)
    }
  }

  private val toJsMap =
    JavaScriptTransformation.makeInstance("(o) => new Map(Object.entries(o))").value

  test("Map round‑trip through hasHashEntries") {
    forAll(genNumObject) { objJson: Json =>
      val out = toJsMap(host(objJson)).value
      val outJ = json(out)
      assert(outJ == objJson)
    }
  }

  private val mapFn = JavaScriptTransformation.makeInstance("(o) => new Map(Object.entries(o))").value

  test("hasHashEntries branch JS Map round‑trips") {
    val obj = Json.obj("k" -> Json.fromInt(42))
    val res = mapFn(host(obj)).value
    val j2: Json = json(res)
    println(res)
    assert(j2 == obj)
  }

  private val toJsSet =
    JavaScriptTransformation.makeInstance("(arr) => new Set(arr)").value

  test("Set round‑trip as array with same distinct elements and order") {
    forAll(genIntArray) { arrJson: Json =>
      val out = toJsSet(host(arrJson)).value
      assert(out.hasIterator && !out.hasArrayElements)

      val outJson = json(out)
      val expected = Json.arr(arrJson.asArray.get.map(_.asNumber.get.toInt.get).distinct.map(Json.fromInt): _*)
      assert(outJson == expected)
    }
  }

  private val setFn = JavaScriptTransformation.makeInstance("(arr) => new Set(arr)").value

  test("hasIterator branch round‑trips Set as distinct‑preserving array") {
    val arr = Json.arr(Json.fromInt(1), Json.fromInt(2), Json.fromInt(2))
    val expected = Json.arr(Json.fromInt(1), Json.fromInt(2))
    val j2: Json = json(setFn(host(arr)).value)
    assert(j2 == expected)
  }

  // Errors
  test("reject JavaScript that is not a function") {
    val res = JavaScriptTransformation.makeInstance("42 + 1")
    assert(res.isLeft)
  }

  test("reject syntax‑invalid JavaScript") {
    val res = JavaScriptTransformation.makeInstance("function ()")
    assert(res.isLeft && res.left.value.getMessage.toLowerCase.contains("syntax"))
  }

  test("runtime error when attempting to mutate frozen globals") {
    val fnText = "(x) => { globalThis.mutated = true; return x; }"
    val tr = JavaScriptTransformation.makeInstance(fnText).value
    val res = tr(host(Json.fromInt(1)))
    assert(res.isLeft)
  }

  test("runtime error when input type is wrong") {
    val fnText = "(x) => x.map(y => y * 2)" // expects array
    val tr = JavaScriptTransformation.makeInstance(fnText).value
    val res = tr(host(Json.fromInt(7))) // pass number instead
    assert(res.isLeft)
  }

  test("runtime error Produce Object out of bounds") {
    val tr = JavaScriptTransformation.makeInstance("that => ({num: that.values[2]})").value
    val jsonInput = Json.obj("values" -> Json.arr(Json.fromInt(0), Json.fromInt(1)))
    val inputValue = host(jsonInput)
    val out = tr(inputValue)
    assert(out.isLeft)
  }

}
