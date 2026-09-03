package com.thatdot.quine.docs

/** Generates the Quine OSS capability reference. */
object GenerateCapabilities extends CapabilityGenerator {

  /** `QuinePersistenceBuilder` stubs out the ClickHouse builder to throw. */
  val shipped: Set[String] = Set("Cassandra", "Empty", "InMemory", "Keyspaces", "MapDb", "RocksDb")
  val notShipped: Set[String] = Set("ClickHouse")
}
