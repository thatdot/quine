package com.thatdot.quine.persistor

import org.apache.pekko.stream.Materializer

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.graph.NamespaceId

class StatelessPrimePersistor(
  persistenceConfig: PersistenceConfig,
  bloomFilterSize: Option[Long],
  create: (PersistenceConfig, NamespaceId) => PersistenceAgent,
  // This class backs any store without cross-restart state (in-memory, empty, test doubles), so
  // callers that know which store they wrap should say so: the slug is surfaced as the persistor
  // type by telemetry and the backpressure API.
  override val slug: String = "stateless",
)(implicit materializer: Materializer, override val logConfig: LogConfig)
    extends UnifiedPrimePersistor(persistenceConfig, bloomFilterSize) {

  protected def agentCreator(persistenceConfig: PersistenceConfig, namespace: NamespaceId): PersistenceAgent =
    create(persistenceConfig, namespace)
}
