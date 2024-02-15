package com.thatdot.quine.persistor

import scala.concurrent.duration.DurationInt

import com.codahale.metrics.{MetricRegistry, NoopMetricRegistry}

import com.thatdot.quine.util.QuineDispatchers

class MapDbPersistorSpec extends PersistenceAgentSpec {

  lazy val persistor: TempMapDbPrimePersistor =
    new TempMapDbPrimePersistor(
      writeAheadLog = false,
      numberPartitions = 1,
      commitInterval = 1.second, // NB this is unused while `writeAheadLog = false
      metricRegistry = new NoopMetricRegistry(),
      persistenceConfig = PersistenceConfig()
    )
}
