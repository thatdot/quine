package com.thatdot.quine.app.util

import org.apache.pekko.http.scaladsl.model.StatusCode

import com.thatdot.quine.app.serialization.ConversionFailure
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.routes.IngestStreamConfiguration
import com.thatdot.quine.util.Log._

object QuineLoggables {
  implicit val logStatusCode: Loggable[StatusCode] = toStringLoggable[StatusCode]
  implicit val LogCypherValue: Loggable[Value] = toStringLoggable[Value]
  implicit val logConversionFailure: Loggable[ConversionFailure] = toStringLoggable[ConversionFailure]
  // implicit val logIngestStreamStatus : Loggable[IngestStreamStatus] = toStringLoggable[IngestStreamStatus]
  implicit val logIngestStreamConfiguration: Loggable[IngestStreamConfiguration] =
    toStringLoggable[IngestStreamConfiguration]
  // implicit val logCqlIdentifier: Loggable[com.datastax.oss.driver.api.core.CqlIdentifier] = toStringLoggable[com.datastax.oss.driver.api.core.CqlIdentifier]

}
