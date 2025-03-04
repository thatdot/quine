package com.thatdot.quine.persistor

import org.apache.pekko.stream.Materializer

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.graph.NamespaceId

class StatelessPrimePersistor(
  persistenceConfig: PersistenceConfig,
  bloomFilterSize: Option[Long],
  create: (PersistenceConfig, NamespaceId) => PersistenceAgent,
)(implicit materializer: Materializer, override val logConfig: LogConfig)
    extends UnifiedPrimePersistor(persistenceConfig, bloomFilterSize) {

  override val slug: String = "stateless"

  protected def agentCreator(persistenceConfig: PersistenceConfig, namespace: NamespaceId): PersistenceAgent =
    create(persistenceConfig, namespace)
}
