package com.thatdot.quine.webapp.components.streams

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.webapp.fields.{FieldPath, PathSeg}

class NoveltyTransformGeneratorTest extends AnyFunSuite with Matchers {

  private def p(ks: String*): FieldPath = FieldPath(ks.toVector.map(PathSeg.Key))
  private def arrP(before: Seq[String], after: Seq[String]): FieldPath =
    FieldPath(before.toVector.map(PathSeg.Key) ++ Vector(PathSeg.Elem) ++ after.toVector.map(PathSeg.Key))

  private val S = """const s = x => String(x ?? "");"""
  private val A = "const a = x => Array.isArray(x) ? x : [];"
  private val V = "const v = xs => xs.filter(x => x != null).map(String);"

  test("tagged: scalar observation components + tag") {
    val g = NoveltyTransformGenerator.generate(
      observationPaths = Vector(p("data", "num"), p("data", "city")),
      tagPath = Some(p("data", "id")),
    )
    g.function shouldBe
    s"that => { $S return { observation: [ s(that.data?.num), s(that.data?.city) ], tag: that.data?.id ?? null }; }"
    g.recordFormat shouldBe "TAGGED"
  }

  test("bare: no tag => BARE, returns the array") {
    val g = NoveltyTransformGenerator.generate(
      observationPaths = Vector(p("data", "num")),
      tagPath = None,
    )
    g.function shouldBe s"that => { $S return [ s(that.data?.num) ]; }"
    g.recordFormat shouldBe "BARE"
  }

  test("a missing parent cannot fail the function: every step after `that` is optional") {
    val g = NoveltyTransformGenerator.generate(observationPaths = Vector(p("data", "user", "id")), tagPath = None)
    g.function shouldBe s"that => { $S return [ s(that.data?.user?.id) ]; }"
  }

  test("keys that are not JS identifiers use quoted bracket access") {
    val g = NoveltyTransformGenerator.generate(
      observationPaths = Vector(p("data", "n.name"), p("data", "user-id"), p("data", "2fa"), p("data", "say \"hi\"")),
      tagPath = Some(p("data", "id(n)")),
    )
    g.function shouldBe
    s"""that => { $S return { observation: [ s(that.data?.["n.name"]), s(that.data?.["user-id"]), """ +
    """s(that.data?.["2fa"]), s(that.data?.["say \"hi\""]) ], tag: that.data?.["id(n)"] ?? null }; }"""
  }

  test("a non-identifier key straight off `that` needs no optional step") {
    val g = NoveltyTransformGenerator.generate(observationPaths = Vector(p("id(n)")), tagPath = None)
    g.function shouldBe s"""that => { $S return [ s(that["id(n)"]) ]; }"""
  }

  test("array of scalars: [*] spreads the array's values, treating a non-array as empty") {
    val g = NoveltyTransformGenerator.generate(
      observationPaths = Vector(arrP(Seq("data", "tags"), Seq.empty)),
      tagPath = Some(p("data", "id")),
    )
    g.function shouldBe
    s"that => { $A $V return { observation: [ ...v(a(that.data?.tags)) ], tag: that.data?.id ?? null }; }"
  }

  test("array of objects: [*].field maps the field out of each element") {
    val g = NoveltyTransformGenerator.generate(
      observationPaths = Vector(arrP(Seq("data", "items"), Seq("name"))),
      tagPath = None,
    )
    g.function shouldBe s"that => { $A $V return [ ...v(a(that.data?.items).map(e => e?.name)) ]; }"
  }

  test("nested arrays flatten to a single level of values") {
    val path = FieldPath(
      Vector(PathSeg.Key("data"), PathSeg.Key("a"), PathSeg.Elem, PathSeg.Key("b"), PathSeg.Elem, PathSeg.Key("c")),
    )
    val g = NoveltyTransformGenerator.generate(observationPaths = Vector(path), tagPath = None)
    g.function shouldBe
    s"that => { $A $V return [ ...v(a(that.data?.a).flatMap(e => a(e?.b).map(e => e?.c))) ]; }"
  }

  test("mixed scalar + array components") {
    val g = NoveltyTransformGenerator.generate(
      observationPaths = Vector(p("data", "country"), arrP(Seq("data", "tags"), Seq.empty)),
      tagPath = Some(p("data", "id")),
    )
    g.function shouldBe
    s"that => { $S $A $V return { observation: [ s(that.data?.country), ...v(a(that.data?.tags)) ], " +
    "tag: that.data?.id ?? null }; }"
  }

  test("a tag path through an array is rejected") {
    an[IllegalArgumentException] should be thrownBy NoveltyTransformGenerator.generate(
      observationPaths = Vector(p("data", "n")),
      tagPath = Some(arrP(Seq("data", "items"), Seq("id"))),
    )
  }
}
