package com.thatdot.quine.persistor

import org.apache.pekko.stream.Materializer

import com.thatdot.quine.graph.NamespaceId

class StatelessPrimePersistor(
  persistenceConfig: PersistenceConfig,
  bloomFilterSize: Option[Long],
  create: (PersistenceConfig, NamespaceId) => PersistenceAgent
)(implicit materializer: Materializer)
    extends UnifiedPrimePersistor(persistenceConfig, bloomFilterSize) {

  protected def agentCreator(persistenceConfig: PersistenceConfig, namespace: NamespaceId): PersistenceAgent =
    create(persistenceConfig, namespace)
}
