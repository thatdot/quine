package com.thatdot.quine.webapp.components.landing

import com.thatdot.quine.webapp.components.landing.V2ApiTypes._

/** Display types used by `OverviewDiagram` and conversion helpers from the V2 API types. */

final case class IngestInfo(name: String, sourceType: String, source: String, status: String, rate: Long)
final case class StandingQueryOutputInfo(
  name: String,
  outputType: String,
  destination: String,
  resultCount: Long,
  kafkaTopic: Option[String] = None,
)
final case class StandingQueryInfo(name: String, status: String, outputs: Seq[StandingQueryOutputInfo])
final case class PersistorInfo(
  name: String,
  status: String,
  writeLatencyMs: Double,
  writeOpsPerSec: Double,
  readLatencyMs: Double,
  readOpsPerSec: Double,
)
object PersistorInfo {
  val empty: PersistorInfo = PersistorInfo(
    name = "",
    status = "Healthy",
    writeLatencyMs = 0.0,
    writeOpsPerSec = 0.0,
    readLatencyMs = 0.0,
    readOpsPerSec = 0.0,
  )
}

object LandingPageData {

  /** Convert V2 API ingest data to the display types used by OverviewDiagram.
    *
    * `sourceType` is the lowercase slug used for icon lookup.
    * `source` is the human-readable display label used on the diagram.
    */
  def fromV2Ingests(ingests: Seq[V2IngestInfo]): Seq[IngestInfo] =
    ingests.map { info =>
      val slug = info.sourceType.toLowerCase
      IngestInfo(
        name = info.name,
        sourceType = slug,
        source = ServiceIcons.labelFor(slug),
        status = info.status,
        rate = info.stats.rates.oneMinute.toLong,
      )
    }

  /** Convert V2 API standing query data to display types used by OverviewDiagram. */
  def fromV2StandingQueries(sqs: Seq[V2StandingQueryInfo]): Seq[StandingQueryInfo] =
    sqs.map { sq =>
      val totalResults = sq.stats.values.map(_.rates.count).sum
      StandingQueryInfo(
        name = sq.name,
        status = if (sq.stats.nonEmpty) "Running" else "Idle",
        outputs = sq.outputs.map { output =>
          val destSlug = output.destinations.headOption.map(_.destinationType.toLowerCase).getOrElse("unknown")
          StandingQueryOutputInfo(
            name = output.name,
            outputType = destSlug,
            // Key by type alone so multiple outputs of the same type collapse to one
            // destination node in the overview diagram.
            destination = ServiceIcons.labelFor(destSlug),
            resultCount = totalResults,
          )
        },
      )
    }
}
