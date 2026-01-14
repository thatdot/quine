package com.thatdot.quine.app.util

import com.thatdot.common.logging.Log.{AlwaysSafeLoggable, Loggable, toStringLoggable}
import com.thatdot.quine.app.model.ingest2.{IngestSource, V2IngestEntities}
import com.thatdot.quine.app.routes.UnifiedIngestConfiguration
import com.thatdot.quine.app.v2api.definitions.ApiCommand
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest
import com.thatdot.quine.routes.{IngestStreamConfiguration, SampleQuery, UiNodeAppearance}
import com.thatdot.quine.serialization.ConversionFailure

object QuineLoggables {
  implicit val logConversionFailure: Loggable[ConversionFailure] = toStringLoggable[ConversionFailure]

  implicit val logIngestStreamConfiguration: AlwaysSafeLoggable[IngestStreamConfiguration] =
    _.toString
  implicit val logQuineIngestConfigurationApi: AlwaysSafeLoggable[ApiIngest.Oss.QuineIngestConfiguration] =
    _.toString
  implicit val logQuineIngestSourceApi: AlwaysSafeLoggable[ApiIngest.IngestSource] =
    _.toString
  implicit val logQuineIngestConfiguration: AlwaysSafeLoggable[V2IngestEntities.QuineIngestConfiguration] =
    _.toString
  implicit val logQuineIngestSource: AlwaysSafeLoggable[IngestSource] =
    _.toString

  implicit val logUnifiedIngestStreamConfiguration: AlwaysSafeLoggable[UnifiedIngestConfiguration] =
    _.config.fold(_.toString, _.toString)
  implicit val logStandingQueryDefinition: AlwaysSafeLoggable[com.thatdot.quine.routes.StandingQueryDefinition] =
    _.toString
  implicit val logStandingQueryDefinition2
    : AlwaysSafeLoggable[com.thatdot.quine.app.v2api.definitions.query.standing.StandingQuery.StandingQueryDefinition] =
    _.toString
  implicit val logRegisteredStandingQuery: AlwaysSafeLoggable[com.thatdot.quine.routes.RegisteredStandingQuery] =
    _.toString
  implicit val logRegisteredStandingQuery2
    : AlwaysSafeLoggable[com.thatdot.quine.app.v2api.definitions.query.standing.StandingQuery.RegisteredStandingQuery] =
    _.toString

  implicit def logStandingQueryOutput[OutputT <: com.thatdot.quine.routes.StandingQueryResultOutputUserDef]
    : AlwaysSafeLoggable[OutputT] = _.toString
  implicit val logStandingQueryResultWorkflow2
    : AlwaysSafeLoggable[com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultWorkflow] =
    _.toString
  implicit val logStatusCode: AlwaysSafeLoggable[org.apache.pekko.http.scaladsl.model.StatusCode] = _.value

  implicit def logApiCommand[C <: ApiCommand]: AlwaysSafeLoggable[C] = _.toString

  implicit val logSampleQuery: AlwaysSafeLoggable[SampleQuery] =
    _.toString
  implicit val logUiNodeAppearance: AlwaysSafeLoggable[UiNodeAppearance] =
    _.toString
  implicit val logUiNodeQuickQuery: AlwaysSafeLoggable[com.thatdot.quine.routes.UiNodeQuickQuery] =
    _.toString
}
