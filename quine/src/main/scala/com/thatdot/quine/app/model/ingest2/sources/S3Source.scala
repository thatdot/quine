package com.thatdot.quine.app.model.ingest2.sources

import java.nio.charset.Charset

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.connectors.s3.scaladsl.S3
import org.apache.pekko.stream.connectors.s3.{S3Attributes, S3Ext, S3Settings}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

import cats.data.ValidatedNel

import com.thatdot.quine.app.model.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.model.ingest.util.AwsOps
import com.thatdot.quine.app.model.ingest2.FileFormat
import com.thatdot.quine.app.model.ingest2.source._
import com.thatdot.quine.app.model.ingest2.sources.FileSource.decodedSourceFromFileStream
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.routes._
import com.thatdot.quine.util.BaseError

case class S3Source(
  format: FileFormat,
  bucket: String,
  key: String,
  credentials: Option[AwsCredentials],
  maximumLineSize: Int,
  charset: Charset = DEFAULT_CHARSET,
  ingestBounds: IngestBounds = IngestBounds(),
  meter: IngestMeter,
  decoders: Seq[ContentDecoder] = Seq(),
)(implicit system: ActorSystem) {

  def decodedSource: ValidatedNel[BaseError, DecodedSource] =
    decodedSourceFromFileStream(
      S3Source.s3Source(bucket, key, credentials),
      format,
      charset,
      maximumLineSize,
      ingestBounds,
      meter,
      decoders,
    )

}

object S3Source {

  def s3Source(bucket: String, key: String, credentials: Option[AwsCredentials])(implicit
    system: ActorSystem,
  ): Source[ByteString, NotUsed] = {
    val src = credentials match {
      case None =>
        S3.getObject(bucket, key)
      case creds @ Some(_) =>
        // TODO: See example: https://stackoverflow.com/questions/61938052/alpakka-s3-connection-issue
        val settings: S3Settings =
          S3Ext(system).settings.withCredentialsProvider(AwsOps.staticCredentialsProvider(creds))
        val attributes = S3Attributes.settings(settings)
        S3.getObject(bucket, key).withAttributes(attributes)
    }
    src.mapMaterializedValue(_ => NotUsed)
  }
}
