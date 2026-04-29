package com.thatdot.api.v2

import io.circe.syntax.EncoderOps
import io.circe.{Json, parser}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PageCodecSpec extends AnyFunSuite with Matchers {

  test("Page omits nextPageToken when there are no further pages") {
    val page = Page.of(List("a", "b", "c"))
    val json = page.asJson
    json.hcursor.downField("items").as[List[String]] shouldBe Right(List("a", "b", "c"))
    json.hcursor.downField("nextPageToken").focus shouldBe None
  }

  test("Page emits nextPageToken when present") {
    val page = Page(List("hello", "world"), Some("next-token"))
    val json = page.asJson
    json.hcursor.get[String]("nextPageToken") shouldBe Right("next-token")
  }

  test("Page.of accepts Set, Seq, Vector, List and converts to a List") {
    Page.of(Set(1, 2, 3)).items.toSet shouldBe Set(1, 2, 3)
    Page.of(Vector("x", "y")).items shouldBe List("x", "y")
    Page.of(Seq(1L, 2L)).items shouldBe List(1L, 2L)
    Page.of(List("only")).items shouldBe List("only")
  }

  test("Page round-trips through encode/decode") {
    val page = Page(List("hello", "world"), Some("next-token"))
    val encoded = page.asJson.noSpaces
    parser.parse(encoded).flatMap(_.as[Page[String]]) shouldBe Right(page)
  }

  test("Page decodes when nextPageToken is omitted") {
    val json = Json.obj("items" -> Json.arr(Json.fromString("a"), Json.fromString("b")))
    json.as[Page[String]] shouldBe Right(Page(List("a", "b"), None))
  }

  test("Page decodes empty-string nextPageToken as None (canonicalize end-of-pages signal)") {
    val json = Json.obj(
      "items" -> Json.arr(Json.fromString("a")),
      "nextPageToken" -> Json.fromString(""),
    )
    json.as[Page[String]] shouldBe Right(Page(List("a"), None))
  }

  test("Page decodes null nextPageToken as None") {
    val json = Json.obj(
      "items" -> Json.arr(Json.fromString("a")),
      "nextPageToken" -> Json.Null,
    )
    json.as[Page[String]] shouldBe Right(Page(List("a"), None))
  }

  test("Empty page encodes as {items: []} with nextPageToken omitted") {
    val page = Page.of(List.empty[Int])
    val json = page.asJson
    json.hcursor.downField("items").as[List[Int]] shouldBe Right(Nil)
    json.hcursor.downField("nextPageToken").focus shouldBe None
  }
}
