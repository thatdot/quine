package com.thatdot.quine.app.routes

import com.codahale.metrics.{Meter, Metered, MetricRegistry}

import com.thatdot.quine.app.Metrics
import com.thatdot.quine.graph.NamespaceId

/** Like [[Metered]], but maintains multiple counters relevant to ingest
  */
sealed abstract class IngestMetered {
  def counts: Metered

  def bytes: Metered

  def getCount: Long = counts.getCount
}
object IngestMetered {

  /** Freeze a copy of the provided ingestMetered (ie, return a copy which will never change)
    * @param im the [[IngestMetered]] to freeze a copy of
    * @return the frozen copy
    */
  def freeze(im: IngestMetered): IngestMetered = new IngestMetered {
    override val counts: Metered = StoppedMeter.fromMeter(im.counts)
    override val bytes: Metered = StoppedMeter.fromMeter(im.bytes)
  }

  /** Returns an ingest meter with meters retrieved or created based on the provided ingest name
    * @see com.codahale.metrics.MetricRegistry#meter
    */
  def ingestMeter(namespaceId: NamespaceId, name: String): IngestMeter =
    IngestMeter(
      name,
      Metrics.meter(mkCountMeterName(namespaceId, name)),
      Metrics.meter(mkBytesMeterName(namespaceId, name))
    )

  /** Removes any meters used in ingest meters for the provided ingest name
    * @see com.codahale.metrics.MetricRegistry#remove
    */
  def removeIngestMeter(namespaceId: NamespaceId, name: String): Boolean =
    Metrics.remove(mkCountMeterName(namespaceId, name)) &&
    Metrics.remove(mkBytesMeterName(namespaceId, name))

  private def ingestMeterName(namespaceId: NamespaceId, name: String, attribute: String): String =
    namespaceId.fold(MetricRegistry.name("ingest", name, attribute))(ns =>
      MetricRegistry.name("ns", ns.name, "ingest", name, attribute)
    )

  private def mkCountMeterName(namespaceId: NamespaceId, name: String): String =
    ingestMeterName(namespaceId, name, attribute = "count")
  private def mkBytesMeterName(namespaceId: NamespaceId, name: String): String =
    ingestMeterName(namespaceId, name, attribute = "bytes")
}

final case class IngestMeter private[routes] (
  name: String,
  countMeter: Meter, // mutable
  bytesMeter: Meter // mutable
) extends IngestMetered {
  def mark(bytes: Int): Unit = {
    countMeter.mark()
    bytesMeter.mark(bytes.toLong)
  }
  override def counts: Metered = countMeter
  override def bytes: Metered = bytesMeter
}

/** Meter that has been halted (so its rates/counts are no longer changing)
  *
  * This is handy for keeping track of rates of a stopped stream (completed or crashed), since we
  * don't want the rates to trend downwards after the stream has stopped.
  */
final case class StoppedMeter(
  getCount: Long,
  getFifteenMinuteRate: Double,
  getFiveMinuteRate: Double,
  getMeanRate: Double,
  getOneMinuteRate: Double
) extends Metered
object StoppedMeter {
  def fromMeter(meter: Metered): Metered = StoppedMeter(
    meter.getCount,
    meter.getFifteenMinuteRate,
    meter.getFiveMinuteRate,
    meter.getMeanRate,
    meter.getOneMinuteRate
  )
}
