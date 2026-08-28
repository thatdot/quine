package com.thatdot.quine.webapp.util

import scala.scalajs.js

import org.scalajs.dom
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Guards the POST/DELETE request shape.
  *
  * This exists because of a bug that compiled cleanly and produced a request with no body at
  * all: `payload.foreach(this.body = _)` written *inside* `new dom.RequestInit { … }` resolved
  * `payload` to `RequestInit`'s own `body` member — the trait's members shadow the enclosing
  * scope there — and lowered to `if (this.body !== undefined) this.body = this.body`. The server
  * answered "Invalid value for: body (exhausted input)".
  *
  * Nothing in the type system catches that, so assert on the constructed object instead.
  */
class QuineApiClientRequestInitTest extends AnyFunSuite with Matchers {

  // A plain dictionary is a valid `HeadersInit`, so these assertions need no `Headers` global
  // (the jsdom test environment has none).
  private val someHeaders: dom.HeadersInit =
    js.Dictionary("Accept" -> "application/json").asInstanceOf[dom.HeadersInit]

  private def initFor(httpMethod: dom.HttpMethod, payload: Option[String]): js.Dynamic =
    QuineApiClient.jsonRequestInit(httpMethod, payload, someHeaders).asInstanceOf[js.Dynamic]

  private def bodyOf(init: js.Dynamic): Option[String] =
    init.body.asInstanceOf[js.UndefOr[String]].toOption

  test("a POST with a payload actually carries it") {
    val json = """{"query":"MATCH (n) RETURN n","destinations":[{"type":"Drop"}]}"""
    val init = initFor(dom.HttpMethod.POST, Some(json))

    bodyOf(init) shouldBe Some(json)
    init.method.asInstanceOf[String] shouldBe "POST"
    init.headers shouldBe someHeaders
  }

  test("a bodiless verb sends no body") {
    // The `…:cancel` custom verb and DELETE: their whole input is the URL, and sending an empty
    // string instead of nothing would be rejected the same way a missing body is.
    bodyOf(initFor(dom.HttpMethod.POST, None)) shouldBe None
    bodyOf(initFor(dom.HttpMethod.DELETE, None)) shouldBe None
  }

  test("Content-Type is sent only when there is a body to describe") {
    QuineApiClient.jsonHeaders(hasBody = true) should contain("Content-Type" -> "application/json")
    QuineApiClient.jsonHeaders(hasBody = false).map(_._1) should not contain "Content-Type"
  }

  test("Accept is always sent, so errors come back as the AIP-193 envelope") {
    Seq(true, false).foreach { hasBody =>
      QuineApiClient.jsonHeaders(hasBody) should contain("Accept" -> "application/json")
    }
  }
}
