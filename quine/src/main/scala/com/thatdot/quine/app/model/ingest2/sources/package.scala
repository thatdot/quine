package com.thatdot.quine.app.model.ingest2

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.connectors.text.scaladsl.TextFlow
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Source}
import org.apache.pekko.stream.{KillSwitches, UniqueKillSwitch}
import org.apache.pekko.util.ByteString

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.model.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.model.ingest2.source.IngestBounds
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.app.{PekkoKillSwitch, ShutdownSwitch}
package object sources extends LazyLogging {

  def withKillSwitches[A](src: Source[A, NotUsed]): Source[A, ShutdownSwitch] =
    src
      .viaMat(KillSwitches.single)(Keep.right)
      .mapMaterializedValue((ks: UniqueKillSwitch) => PekkoKillSwitch(ks))

  val DEFAULT_CHARSET: Charset = Charset.forName("UTF-8")
  val DEFAULT_MAXIMUM_LINE_SIZE: Int = 1000

  def decompressingFlow(decoders: Seq[ContentDecoder]): Flow[ByteString, ByteString, NotUsed] =
    ContentDecoder.decoderFlow(decoders)

  def metered[A](meter: IngestMeter, sizeOf: A => Int): Flow[A, A, NotUsed] =
    Flow[A].wireTap(bs => meter.mark(sizeOf(bs)))

  def transcodingFlow(charset: Charset): Flow[ByteString, ByteString, NotUsed] = charset match {
    case StandardCharsets.UTF_8 | StandardCharsets.ISO_8859_1 | StandardCharsets.US_ASCII =>
      Flow[ByteString]
    case otherCharset =>
      logger.warn(
        s"Charset-sensitive ingest does not directly support $otherCharset - transcoding through UTF-8 first",
      )
      TextFlow.transcoding(otherCharset, StandardCharsets.UTF_8)
  }

  def boundingFlow[A](ingestBounds: IngestBounds): Flow[A, A, NotUsed] =
    ingestBounds.ingestLimit.fold(Flow[A].drop(ingestBounds.startAtOffset))(limit =>
      Flow[A].drop(ingestBounds.startAtOffset).take(limit),
    )

}
