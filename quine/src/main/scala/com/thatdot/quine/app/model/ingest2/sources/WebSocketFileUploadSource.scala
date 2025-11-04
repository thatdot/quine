package com.thatdot.quine.app.model.ingest2.sources

import scala.util.{Success, Try}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.apache.pekko.util.ByteString

import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.app.ShutdownSwitch
import com.thatdot.quine.app.model.ingest2.source.DecodedSource
import com.thatdot.quine.app.model.ingest2.sources
import com.thatdot.quine.app.routes.IngestMeter

trait PushHub {
  type Element
  def sink: Sink[Element, NotUsed]
  val source: Source[Element, NotUsed]
}

trait DecodingFoldableFrom {
  type Element
  def decodingFlow: Flow[ByteString, Element, NotUsed]
  val dataFoldableFrom: DataFoldableFrom[Element]
}

trait DecodingHub extends PushHub with DecodingFoldableFrom

class WebSocketFileUploadSource(
  meter: IngestMeter,
  val decodingHub: DecodingHub,
) extends DecodedSource(meter) {

  override type Decoded = decodingHub.Element
  override type Frame = decodingHub.Element
  override val foldableFrame: DataFoldableFrom[Frame] = decodingHub.dataFoldableFrom
  override val foldable: DataFoldableFrom[Decoded] = decodingHub.dataFoldableFrom

  // We can't meaningfully pass along frames we fail to decode, since we only get the element if decoding is successful
  override def content(input: Frame): Array[Byte] = Array.emptyByteArray

  /** Stream of decoded values. This stream must already be metered. */
  override def stream: Source[(() => Try[Decoded], Frame), ShutdownSwitch] =
    sources.withKillSwitches(decodingHub.source.map(element => (() => Success(element), element)))
}
