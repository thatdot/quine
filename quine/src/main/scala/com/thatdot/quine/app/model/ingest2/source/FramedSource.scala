package com.thatdot.quine.app.model.ingest2.source

import scala.util.Try

import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.{Done, NotUsed}

import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.app.ShutdownSwitch
import com.thatdot.quine.app.model.ingest2.codec.FrameDecoder
import com.thatdot.quine.app.routes.IngestMeter

/** Define a source in terms of Frames it can return.
  *
  * A Frame is a chunk of an original data source that contains
  * an Array of bytes representing a single element.
  * To retrieve decoded values a FramedSource must be paired with a [[com.thatdot.quine.app.ingest2.codec.FrameDecoder]].
  *
  * Frames are defined by the ingest type, e.g.
  * - Kafka: Content delimited
  * - SQS:message
  * - Kinesis: Record
  *
  * Decoded Formats
  * - CSV with header (Map[String,String])
  * - CSV rows (Iterable[String])
  * - Json
  * - String
  * - Drop (ignoring)
  * - Array[Byte]
  * - Protobuf Dynamic Message
  *
  *  The stream defined as a part of a framed source must have metering
  *  as well as any stream features already applied.
  */

trait FramedSource {
  type SrcFrame

  val stream: Source[SrcFrame, ShutdownSwitch]
  val meter: IngestMeter

  def content(input: SrcFrame): Array[Byte]

  def foldableFrame: DataFoldableFrom[SrcFrame]

  /** Note that the ack flow is only applied at the usage site (e.g. directly
    * in quine/novelty). This is because the ack is applied after the platform
    * specific use (e.g. insert into graph).
    */
  val ack: Flow[SrcFrame, Done, NotUsed] = Flow.fromFunction(_ => Done)

  /** Close any associated resources after terminating the stream. */
  def onTermination(): Unit = ()

  /** Pair a framed source with a decoder in order to interpret the raw
    * frame data.
    *
    * Any type for which there is a decoder is foldable into
    * common types.
    */
  def toDecoded[DecodedA](decoder: FrameDecoder[DecodedA]): DecodedSource =
    new DecodedSource(meter) {
      type Decoded = DecodedA
      type Frame = SrcFrame

      val foldableFrame: DataFoldableFrom[SrcFrame] = FramedSource.this.foldableFrame
      val foldable: DataFoldableFrom[Decoded] = decoder.foldable

      override def content(input: SrcFrame): Array[Byte] = FramedSource.this.content(input)

      private val deserializationTimer = this.meter.unmanagedDeserializationTimer

      def stream: Source[(() => Try[Decoded], Frame), ShutdownSwitch] =
        FramedSource.this.stream.map { envelope =>
          val timer = deserializationTimer.time()
          val decoded = () => decoder.decode(content(envelope))
          decoded().foreach(_ => timer.stop()) // only time successful deserializations
          decoded -> envelope
        }

      override def ack: Flow[SrcFrame, Done, NotUsed] = FramedSource.this.ack

      override def onTermination(): Unit = FramedSource.this.onTermination()

    }
}

object FramedSource {

  /** Construct a framed source from a raw stream of frames along with a definition of how to extract
    * bytes from the frame.
    *
    * Any features this source supports must be applied before calling this method.
    */
  def apply[Frame](
    source: Source[Frame, ShutdownSwitch],
    ingestMeter: IngestMeter,
    decodeFrame: Frame => Array[Byte],
    foldableFrameInp: DataFoldableFrom[Frame],
    ackFlow: Flow[Frame, Done, NotUsed] = Flow.fromFunction[Frame, Done](_ => Done),
    terminationHook: () => Unit = () => (),
  ): FramedSource =
    new FramedSource {
      type SrcFrame = Frame
      val stream: Source[Frame, ShutdownSwitch] = source
      val meter: IngestMeter = ingestMeter

      override def content(input: Frame): Array[Byte] = decodeFrame(input)
      override val foldableFrame: DataFoldableFrom[SrcFrame] = foldableFrameInp
      override def onTermination(): Unit = terminationHook()

      override val ack = ackFlow

    }

}
