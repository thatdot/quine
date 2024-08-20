package com.thatdot.quine.app.ingest2.sources

import java.nio.charset.Charset

import scala.concurrent.Future

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.connectors.s3.scaladsl.S3
import org.apache.pekko.stream.connectors.s3.{ObjectMetadata, S3Attributes, S3Ext, S3Settings}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

import com.thatdot.quine.app.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.ingest.util.AwsOps
import com.thatdot.quine.app.ingest2.source._
import com.thatdot.quine.app.ingest2.sources.FileSource.decodedSourceFromFileStream
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.routes._

case class S3Source(
  format: FileIngestFormat,
  bucket: String,
  key: String,
  credentials: Option[AwsCredentials],
  maximumLineSize: Int,
  charset: Charset = DEFAULT_CHARSET,
  bounds: IngestBounds = IngestBounds(),
  meter: IngestMeter,
  decoders: Seq[ContentDecoder] = Seq()
)(implicit system: ActorSystem) {
  val src: Source[ByteString, Future[ObjectMetadata]] = credentials match {
    case None =>
      S3.getObject(bucket, key)
    case creds @ Some(_) =>
      // TODO: See example: https://stackoverflow.com/questions/61938052/alpakka-s3-connection-issue
      val settings: S3Settings =
        S3Ext(system).settings.withCredentialsProvider(AwsOps.staticCredentialsProvider(creds))
      val attributes = S3Attributes.settings(settings)
      S3.getObject(bucket, key).withAttributes(attributes)
  }

  def decodedSource: DecodedSource =
    decodedSourceFromFileStream(
      src.mapMaterializedValue(_ => NotUsed),
      format,
      charset,
      maximumLineSize,
      bounds,
      meter,
      decoders
    )

}
