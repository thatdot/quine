package com.thatdot.quine.webapp.fields

import io.circe.Json
import io.circe.parser.parse
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FieldSchemaTest extends AnyFunSuite with Matchers {

  private def j(s: String): Json = parse(s).fold(e => fail(s"bad json: $e"), identity)

  private def path(segs: PathSeg*): FieldPath = FieldPath(segs.toVector)
  private def key(ks: String*): FieldPath = FieldPath(ks.toVector.map(PathSeg.Key))

  private def leafPaths(schema: FieldSchema): Set[String] =
    schema.leaves.map(_._1.render).toSet

  test("flat object: leaves are the top-level scalar keys") {
    val s = FieldSchema.fromSamples(List(j("""{"id":"a","num":1,"ok":true}""")))
    s.sampleCount shouldBe 1
    leafPaths(s) shouldBe Set("id", "num", "ok")
    s.stats(key("id")).kinds shouldBe Set(JsonKind.Str)
    s.stats(key("num")).kinds shouldBe Set(JsonKind.Num)
    s.stats(key("ok")).kinds shouldBe Set(JsonKind.Bool)
  }

  test("SQ-result envelope shape: nested data.* and meta.* leaves") {
    val s = FieldSchema.fromSamples(
      List(j("""{"meta":{"isPositiveMatch":true},"data":{"id":"n1","num":42}}""")),
    )
    leafPaths(s) shouldBe Set("meta.isPositiveMatch", "data.id", "data.num")
    // interior object paths exist but are not leaves
    s.stats(key("data")).kinds shouldBe Set(JsonKind.Obj)
    s.stats.contains(key("data")) shouldBe true
    s.leaves.map(_._1).contains(key("data")) shouldBe false
  }

  test("presence counts reflect optional fields across samples") {
    val s = FieldSchema.fromSamples(
      List(
        j("""{"data":{"id":"a","city":"NYC"}}"""),
        j("""{"data":{"id":"b"}}"""),
        j("""{"data":{"id":"c","city":"LA"}}"""),
      ),
    )
    s.sampleCount shouldBe 3
    s.stats(key("data", "id")).presentIn shouldBe 3
    s.stats(key("data", "city")).presentIn shouldBe 2
  }

  test("heterogeneous field is flagged mixed; nullable-but-consistent is not") {
    val s = FieldSchema.fromSamples(
      List(
        j("""{"data":{"x":1,"y":"s"}}"""),
        j("""{"data":{"x":"now-a-string","y":null}}"""),
      ),
    )
    s.stats(key("data", "x")).isMixed shouldBe true // number then string
    s.stats(key("data", "y")).isMixed shouldBe false // string then null -> not mixed
    s.stats(key("data", "y")).kinds shouldBe Set(JsonKind.Str, JsonKind.Null)
  }

  test("arrays descend under a single [*] wildcard, merging element shapes") {
    val s = FieldSchema.fromSamples(
      List(j("""{"data":{"items":[{"name":"a","qty":1},{"name":"b","qty":2}]}}""")),
    )
    // the array itself is an interior Arr path, not a leaf
    s.stats(key("data", "items")).kinds shouldBe Set(JsonKind.Arr)
    // element fields are leaves under [*]
    leafPaths(s) should contain("data.items[*].name")
    leafPaths(s) should contain("data.items[*].qty")
    // presence is per-sample (not per-element): one sample -> presentIn 1 even with two elements
    s.stats(path(PathSeg.Key("data"), PathSeg.Key("items"), PathSeg.Elem, PathSeg.Key("name"))).presentIn shouldBe 1
  }

  test("array of scalars: [*] is a scalar leaf") {
    val s = FieldSchema.fromSamples(List(j("""{"data":{"tags":["x","y"]}}""")))
    leafPaths(s) should contain("data.tags[*]")
    s.stats(path(PathSeg.Key("data"), PathSeg.Key("tags"), PathSeg.Elem)).kinds shouldBe Set(JsonKind.Str)
  }

  test("examples are bounded and distinct") {
    val samples = (1 to 10).map(i => j(s"""{"data":{"n":${i % 2}}}""")).toList
    val s = FieldSchema.fromSamples(samples)
    val ex = s.stats(key("data", "n")).examples
    ex.size should be <= 3
    ex.distinct shouldBe ex
  }

  test("observe is incremental and equivalent to fromSamples") {
    val samples = List(
      j("""{"data":{"id":"a","num":1}}"""),
      j("""{"data":{"id":"b","extra":true}}"""),
    )
    val incremental = samples.foldLeft(FieldSchema.empty)(FieldSchema.observe)
    val batch = FieldSchema.fromSamples(samples)
    incremental shouldBe batch
  }

  test("render") {
    key("data", "user", "id").render shouldBe "data.user.id"
    path(
      PathSeg.Key("data"),
      PathSeg.Key("items"),
      PathSeg.Elem,
      PathSeg.Key("name"),
    ).render shouldBe "data.items[*].name"
    FieldPath.root.render shouldBe "."
  }

  test("render quotes keys that are not plain names, so distinct paths never render alike") {
    key("data", "a.b").render shouldBe "data[\"a.b\"]"
    key("data", "a", "b").render shouldBe "data.a.b"
    key("id(n)").render shouldBe "[\"id(n)\"]"
    key("data", "x[*]").render shouldBe "data[\"x[*]\"]"
  }

  test("parse reads back what render writes") {
    val paths = List(
      key("data", "user", "id"),
      key("data", "n.name"),
      key("id(n)"),
      key("data", "say \"hi\""),
      FieldPath(Vector(PathSeg.Key("data"), PathSeg.Key("items"), PathSeg.Elem, PathSeg.Key("name"))),
      FieldPath(Vector(PathSeg.Key("data"), PathSeg.Key("x"), PathSeg.Elem, PathSeg.Elem)),
    )
    paths.foreach(p => FieldPath.parse(p.render) shouldBe Right(p))
  }

  test("parse rejects what isn't a path") {
    List("", "  ", ".data", "data.", "data..x", "data.n name", "data[", "data[\"x", "data[0]").foreach { text =>
      withClue(text)(FieldPath.parse(text).isLeft shouldBe true)
    }
  }

  test("a field added by hand is a pickable leaf, counted in no sample") {
    val schema = FieldSchema.declare(FieldSchema.empty, key("data", "city"), JsonKind.Str)
    schema.sampleCount shouldBe 0
    schema.leaves.map(_._1) shouldBe Vector(key("data", "city"))
    schema.stats(key("data", "city")).declared shouldBe true
  }

  test("a field added by hand stays marked once samples show it, and keeps what they teach") {
    val declared = FieldSchema.declare(FieldSchema.empty, key("data", "n"), JsonKind.Str)
    val sampled = FieldSchema.observe(declared, Json.obj("data" -> Json.obj("n" -> Json.fromInt(1))))
    val stat = sampled.stats(key("data", "n"))
    stat.declared shouldBe true
    stat.presentIn shouldBe 1
    stat.kinds shouldBe Set(JsonKind.Str, JsonKind.Num)
  }
}
