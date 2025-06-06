package com.thatdot.quine.app.config


import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

import com.thatdot.quine.util.{Host, Port}

class WebServerConfigTest extends AnyFunSuite with should.Matchers {
  
  test("WebServerBindConfig should use localhost for wildcard bindings") {
    val wildcardConfig = WebServerBindConfig(
      address = Host("0.0.0.0"),
      port = Port(8080)
    )
    
    val url = wildcardConfig.guessResolvableUrl
    url.toString should startWith("http://127.0.0.1:8080")
  }
  
  test("WebServerBindConfig should construct URL from host/port") {
    val config = WebServerBindConfig(
      address = Host("127.0.0.1"),
      port = Port(8080)
    )
    
    val url = config.guessResolvableUrl
    url.toString shouldEqual "http://127.0.0.1:8080"
  }
  
  
  
  test("WebserverAdvertiseConfig should use path when provided") {
    val configWithPath = WebserverAdvertiseConfig(
      address = Host("example.com"),
      port = Port(8080),
      path = Some("/webapp")
    )
    
    val url = configWithPath.url("https")
    url.toString shouldEqual "https://example.com:8080/webapp"
  }
  
  test("WebserverAdvertiseConfig should construct URL from host/port when path is not provided") {
    val configWithoutPath = WebserverAdvertiseConfig(
      address = Host("example.com"),
      port = Port(8080),
      path = None
    )
    
    val url = configWithoutPath.url("https")
    url.toString shouldEqual "https://example.com:8080"
  }
  
  test("WebserverAdvertiseConfig should respect the protocol parameter when path is not provided") {
    val config = WebserverAdvertiseConfig(
      address = Host("example.com"),
      port = Port(8080)
    )
    
    val httpUrl = config.url("http")
    httpUrl.toString shouldEqual "http://example.com:8080"
    
    val httpsUrl = config.url("https")
    httpsUrl.toString shouldEqual "https://example.com:8080"
  }
}