package com.thatdot.quine.app.util

import com.thatdot.quine.app.routes.UnifiedIngestConfiguration
import com.thatdot.quine.app.serialization.ConversionFailure
import com.thatdot.quine.app.v2api.definitions.ApiCommand
import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities.QuineIngestConfiguration
import com.thatdot.quine.routes.{IngestStreamConfiguration, SampleQuery, UiNodeAppearance}
import com.thatdot.quine.util.Log._

object QuineLoggables {
  implicit val logConversionFailure: Loggable[ConversionFailure] = toStringLoggable[ConversionFailure]
  // implicit val logIngestStreamStatus : Loggable[IngestStreamStatus] = toStringLoggable[IngestStreamStatus]
  implicit val logIngestStreamConfiguration: AlwaysSafeLoggable[IngestStreamConfiguration] =
    _.toString
  implicit val logQuineIngestConfiguration: AlwaysSafeLoggable[QuineIngestConfiguration] =
    _.toString
  // implicit val logCqlIdentifier: Loggable[com.datastax.oss.driver.api.core.CqlIdentifier] = toStringLoggable[com.datastax.oss.driver.api.core.CqlIdentifier]

  implicit val logUnifiedIngestStreamConfiguration: AlwaysSafeLoggable[UnifiedIngestConfiguration] =
    _.config.fold(_.toString, _.toString)
  implicit val LogStandingQueryDefinition: AlwaysSafeLoggable[com.thatdot.quine.routes.StandingQueryDefinition] =
    _.toString
  implicit val LogRegisteredStandingQuery: AlwaysSafeLoggable[com.thatdot.quine.routes.RegisteredStandingQuery] =
    _.toString
  implicit def kogStandingQueryOutput[OutputT <: com.thatdot.quine.routes.StandingQueryResultOutputUserDef]
    : AlwaysSafeLoggable[OutputT] =
    _.toString
  implicit val LogStatusCode: AlwaysSafeLoggable[org.apache.pekko.http.scaladsl.model.StatusCode] = _.value

  implicit def logApiCommand[C <: ApiCommand]: AlwaysSafeLoggable[C] = _.toString

  implicit val LogSampleQuery: AlwaysSafeLoggable[SampleQuery] =
    _.toString
  implicit val LogUiNodeAppearance: AlwaysSafeLoggable[UiNodeAppearance] =
    _.toString
  implicit val LogUiNodeQuickQuery: AlwaysSafeLoggable[com.thatdot.quine.routes.UiNodeQuickQuery] =
    _.toString
}
