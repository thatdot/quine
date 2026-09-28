package com.thatdot.quine.webapp.v2api

import scala.scalajs.js

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.quine.routes.exts.NamespaceParameter
import com.thatdot.quine.routes.{ClientRoutes, CypherQuery}

/** Guards the wire spelling of the historical-moment parameter on the v2 REST query endpoints.
  *
  * The v2 server reads `atTime` as an RFC 3339 string. Because that parameter is optional, a
  * request carrying the v1 spelling (`at-time=<epoch millis>`) is not rejected: it is answered
  * as if no moment had been selected, so a historical query silently returns present-day data.
  * Nothing in the type system distinguishes the two spellings, so assert on the rendered URL.
  */
class V2QueryUiRoutesAtTimeTest extends AnyFunSuite with Matchers {

  private val routes = new ClientRoutes(js.undefined)

  private val millis = 1790083243762L
  private val rfc3339 = "2026-09-22T13:20:43.762Z"
  private val query = CypherQuery("MATCH (n) RETURN n")
  private val ns = NamespaceParameter.defaultNamespaceParameter

  private def queryParams(href: String): Map[String, String] =
    href.split('?').toList match {
      case _ :: qs :: Nil =>
        qs.split('&')
          .toList
          .map { kv =>
            val (k, v) = kv.span(_ != '=')
            k -> js.URIUtils.decodeURIComponent(v.drop(1))
          }
          .toMap
      case _ => Map.empty
    }

  private val endpoints = Seq(
    "cypher:query" -> routes.cypherPostV2,
    "cypher:queryNodes" -> routes.cypherNodesPostV2,
    "cypher:queryEdges" -> routes.cypherEdgesPostV2,
  )

  test("a selected moment is sent as `atTime` in RFC 3339") {
    endpoints.foreach { case (name, endpoint) =>
      val href = endpoint.href((Some(millis), None, ns, query))
      withClue(s"$name -> $href: ") {
        href should include(s"/api/v2/graph/quine/$name?")
        queryParams(href) shouldBe Map("atTime" -> rfc3339)
      }
    }
  }

  test("the v1 `at-time` spelling never appears on a v2 URL") {
    endpoints.foreach { case (name, endpoint) =>
      val href = endpoint.href((Some(millis), None, ns, query))
      withClue(s"$name -> $href: ")(href should not include "at-time")
    }
  }

  test("no selected moment means no `atTime` parameter at all") {
    endpoints.foreach { case (name, endpoint) =>
      val href = endpoint.href((None, None, ns, query))
      withClue(s"$name -> $href: ")(queryParams(href) should not contain key("atTime"))
    }
  }

  test("the RFC 3339 codec round-trips epoch millis") {
    routes.atTimeRfc3339Codec.encode(Some(millis)) shouldBe Some(rfc3339)
    routes.atTimeRfc3339Codec.decode(Some(rfc3339)) shouldBe endpoints4s.Valid(Some(millis))
    routes.atTimeRfc3339Codec.decode(None) shouldBe endpoints4s.Valid(None)
  }

  test("the RFC 3339 codec rejects text that is not a timestamp") {
    routes.atTimeRfc3339Codec.decode(Some("not-a-timestamp")) shouldBe a[endpoints4s.Invalid]
  }

  test("the RFC 3339 codec refuses a moment a JavaScript Date cannot represent") {
    an[IllegalArgumentException] should be thrownBy routes.atTimeRfc3339Codec.encode(Some(Long.MaxValue))
  }
}
