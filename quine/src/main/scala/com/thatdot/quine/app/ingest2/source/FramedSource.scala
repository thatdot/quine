package com.thatdot.quine.app.ingest2.source

import scala.util.Try

import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.{Done, NotUsed}

import com.thatdot.quine.app.ShutdownSwitch
import com.thatdot.quine.app.ingest2.codec.FrameDecoder
import com.thatdot.quine.app.ingest2.core.DataFoldableFrom
import com.thatdot.quine.app.routes.IngestMeter

/** Define a source in terms of Frames it can return.
  *
  * A Frame is a chunk of an original data source that contains
  * an Array of bytes representing a single element.
  *
  * Frames are defined by the ingest type, e.g.
  * - Kafka: Content delimited
  * - SQS:message
  * - Kinesis: Record
  *
  * Decoded Formats
  * - CSV with header
  * - CSV with static header
  * - Json
  * - String
  * - cypher.Null (ignoring)
  * - Array[Byte]
  * - Protobuf (with schema)
  */
abstract class FramedSource[SrcFrame](val stream: Source[SrcFrame, ShutdownSwitch], val meter: IngestMeter) {
  def content(input: SrcFrame): Array[Byte]

  def ack: Flow[SrcFrame, Done, NotUsed] = Flow.fromFunction(_ => Done)

  def onTermination(): Unit = ()

  def toDecoded[DecodedA](decoder: FrameDecoder[DecodedA]): DecodedSource =
    new DecodedSource(meter) {
      type Decoded = DecodedA
      type Frame = SrcFrame
      val foldable: DataFoldableFrom[Decoded] = decoder.foldable

      def stream: Source[(Try[Decoded], Frame), ShutdownSwitch] =
        FramedSource.this.stream.map(envelope => (decoder.decode(content(envelope)), envelope))

      override def ack: Flow[SrcFrame, Done, NotUsed] = FramedSource.this.ack

      override def onTermination(): Unit = FramedSource.this.onTermination()

    }
}
