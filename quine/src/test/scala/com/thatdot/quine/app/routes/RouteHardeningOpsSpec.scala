package com.thatdot.quine.app.routes

import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.complete
import org.apache.pekko.http.scaladsl.server.StandardRoute
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RouteHardeningOpsSpec extends AnyFlatSpec with Matchers with ScalatestRouteTest {
  import Util.RouteHardeningOps.syntax._

  private val okRoute: StandardRoute = complete("OK")

  "withXssHardening" should "add Content-Security-Policy header" in {
    Get("/") ~> okRoute.withXssHardening ~> check {
      val cspValue = header("Content-Security-Policy").get.value()
      cspValue should include("default-src 'self'")
      cspValue should include("script-src 'self' 'unsafe-eval'")
      cspValue should include("object-src 'none'")
      cspValue should include("style-src 'self' 'unsafe-inline'")
      cspValue should include("img-src 'self' data:")
      cspValue should include("media-src 'none'")
      cspValue should include("frame-src 'none'")
      cspValue should include("font-src 'self'")
      cspValue should include("connect-src 'self'")
      cspValue should include("frame-ancestors 'self'")
    }
  }

  "withFrameEmbedHardening" should "add X-Frame-Options header" in {
    Get("/") ~> okRoute.withFrameEmbedHardening ~> check {
      header("X-Frame-Options").get.value() shouldEqual "SAMEORIGIN"
    }
  }

  "withHstsHardening" should "add Strict-Transport-Security header with 1 week max-age" in {
    Get("/") ~> okRoute.withHstsHardening ~> check {
      val hstsValue = header("Strict-Transport-Security").get.value()
      hstsValue should include("max-age=604800")
      hstsValue should include("includeSubDomains")
      hstsValue should not include "preload"
    }
  }

  "withSecurityHardening" should "add all security headers" in {
    Get("/") ~> okRoute.withSecurityHardening ~> check {
      status shouldEqual StatusCodes.OK
      header("Content-Security-Policy") shouldBe defined
      header("X-Frame-Options") shouldBe defined
      header("Strict-Transport-Security") shouldBe defined
    }
  }

  "withSecurityHardening" should "preserve route functionality" in {
    val createdResourceRoute = complete(StatusCodes.Created, "Resource created")
    Get("/") ~> createdResourceRoute.withSecurityHardening ~> check {
      status shouldEqual StatusCodes.Created
      responseAs[String] shouldEqual "Resource created"
    }
  }

  "security hardening methods" should "be chainable individually" in {
    Get("/") ~> okRoute.withXssHardening.withFrameEmbedHardening.withHstsHardening ~> check {
      header("Content-Security-Policy") shouldBe defined
      header("X-Frame-Options") shouldBe defined
      header("Strict-Transport-Security") shouldBe defined
    }
  }
}
