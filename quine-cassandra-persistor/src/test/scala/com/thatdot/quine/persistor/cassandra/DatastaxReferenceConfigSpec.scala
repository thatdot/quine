package com.thatdot.quine.persistor.cassandra

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Guards the DataStax driver config the persistor ships in its own `reference.conf`.
  *
  * This is a placement/regression test: it confirms the persistor module's `reference.conf` is on the
  * classpath, parses, and (per the `appendProjectsLast` assembly strategy and project-first classpath
  * ordering in dev) overrides the driver's own `reference.conf` defaults. If these settings ever
  * silently fall off the classpath -- the fragility that motivated moving them out of the OSS app --
  * this fails.
  */
class DatastaxReferenceConfigSpec extends AnyFunSuite with Matchers {

  test("the persistor reference.conf configures the request throttler and connection limit") {
    val advanced = ConfigFactory.load().getConfig("datastax-java-driver.advanced")

    // Overrides the driver default of PassThroughRequestThrottler. The specific bounds are tunables,
    // so assert only that they are present and sane -- this guards placement and override ordering,
    // not the chosen numbers.
    advanced.getString("throttler.class") shouldBe "ConcurrencyLimitingRequestThrottler"
    advanced.getInt("throttler.max-concurrent-requests") should be > 0
    advanced.getInt("throttler.max-queue-size") should be > 0
    // Unlike the throttler bounds, this one has a hard constraint rather than a preference: the
    // protocol allots 32768 stream ids per connection, and the driver rejects anything at or above
    // that. Asserted so a value chosen as a throughput knob cannot silently exceed it again.
    val maxRequestsPerConnection = advanced.getInt("connection.max-requests-per-connection")
    maxRequestsPerConnection should be > 0
    maxRequestsPerConnection should be < 32768
  }
}
