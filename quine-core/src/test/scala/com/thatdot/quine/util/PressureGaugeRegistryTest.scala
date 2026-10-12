package com.thatdot.quine.util

import org.scalatest.funsuite.AnyFunSuite

class PressureGaugeRegistryTest extends AnyFunSuite {

  private val key = GaugeKey("ingest", "default", "orders", "source")

  private def gauge(): PressureGauge.State = new PressureGauge.State {
    override val name: String = "test-gauge"
    override def isUnderPressure: Boolean = false
  }

  test("deregistering a gauge removes it") {
    val registry = new PressureGaugeRegistry
    val g = gauge()
    registry.register(key, g)
    registry.deregister(key, g)
    assert(!registry.snapshot().contains(key))
  }

  test("a replaced gauge cannot deregister the gauge that replaced it") {
    val registry = new PressureGaugeRegistry
    val deletedIngestsGauge = gauge()
    val newIngestsGauge = gauge()
    registry.register(key, deletedIngestsGauge)
    registry.register(key, newIngestsGauge)
    // The deleted ingest's stream finishes draining after the new, same-named ingest has started.
    registry.deregister(key, deletedIngestsGauge)
    assert(registry.snapshot().contains(key))
    registry.deregister(key, newIngestsGauge)
    assert(!registry.snapshot().contains(key))
  }

  test("deregistering by prefix still removes every gauge of a stream") {
    val registry = new PressureGaugeRegistry
    registry.register(GaugeKey("sq-output", "default", "q/out1", "source"), gauge())
    registry.register(GaugeKey("sq-output", "default", "q1/out1", "source"), gauge())
    registry.deregisterByPrefix("sq-output", "default", "q")
    assert(registry.snapshot().keySet == Set(GaugeKey("sq-output", "default", "q1/out1", "source")))
  }
}
