package com.thatdot.quine.v2api

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.quine.ScalaPrimitiveGenerators
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest._
import com.thatdot.quine.app.v2api.definitions.ingest2.DeadLetterQueueSettings

object V2IngestEndpointGenerators {

  import ScalaPrimitiveGenerators.Gens.{nonEmptyAlphaNumStr, smallPosNum}

  object Gens {

    // Simple IngestSource subtypes for testing
    val numberIterator: Gen[IngestSource.NumberIterator] = for {
      startOffset <- Gen.chooseNum(0L, 1000L)
      limit <- Gen.option(Gen.chooseNum(1L, 1000L))
    } yield IngestSource.NumberIterator(startOffset, limit)

    // Use NumberIterator as representative IngestSource for tests
    val ingestSource: Gen[IngestSource] = numberIterator

    val transformation: Gen[Transformation.JavaScript] = for {
      function <- nonEmptyAlphaNumStr.map(s => s"that => $s")
    } yield Transformation.JavaScript(function)

    val optTransformation: Gen[Option[Transformation]] = Gen.option(transformation)

    val recordRetrySettings: Gen[RecordRetrySettings] = for {
      minBackoff <- Gen.chooseNum(100, 5000)
      maxBackoff <- Gen.chooseNum(10, 60)
      randomFactor <- Gen.chooseNum(0.0, 1.0)
      maxRetries <- Gen.chooseNum(1, 10)
    } yield RecordRetrySettings(minBackoff, maxBackoff, randomFactor, maxRetries)

    val deadLetterQueueSettings: Gen[DeadLetterQueueSettings] = Gen.const(DeadLetterQueueSettings())

    val onRecordErrorHandler: Gen[OnRecordErrorHandler] = for {
      retrySettings <- Gen.option(recordRetrySettings)
      logRecord <- Gen.oneOf(true, false)
      dlqSettings <- deadLetterQueueSettings
    } yield OnRecordErrorHandler(retrySettings, logRecord, dlqSettings)

    val onStreamErrorHandler: Gen[OnStreamErrorHandler] = Gen.oneOf(
      Gen.const(LogStreamError),
      Gen.chooseNum(1, 5).map(RetryStreamError),
    )

    val quineIngestConfiguration: Gen[Oss.QuineIngestConfiguration] = for {
      name <- nonEmptyAlphaNumStr
      source <- ingestSource
      query <- nonEmptyAlphaNumStr.map(s => s"MATCH (n) WHERE id(n) = idFrom($$that) SET n.value = $s")
      parameter <- Gen.oneOf("that", "input", "data")
      transformation <- optTransformation
      parallelism <- smallPosNum
      maxPerSecond <- Gen.option(Gen.chooseNum(1, 1000))
      onRecordError <- onRecordErrorHandler
      onStreamError <- onStreamErrorHandler
    } yield Oss.QuineIngestConfiguration(
      name = name,
      source = source,
      query = query,
      parameter = parameter,
      transformation = transformation,
      parallelism = parallelism,
      maxPerSecond = maxPerSecond,
      onRecordError = onRecordError,
      onStreamError = onStreamError,
    )
  }

  object Arbs {
    implicit val numberIterator: Arbitrary[IngestSource.NumberIterator] = Arbitrary(Gens.numberIterator)
    implicit val ingestSource: Arbitrary[IngestSource] = Arbitrary(Gens.ingestSource)
    implicit val transformation: Arbitrary[Transformation.JavaScript] = Arbitrary(Gens.transformation)
    implicit val onRecordErrorHandler: Arbitrary[OnRecordErrorHandler] = Arbitrary(Gens.onRecordErrorHandler)
    implicit val onStreamErrorHandler: Arbitrary[OnStreamErrorHandler] = Arbitrary(Gens.onStreamErrorHandler)
    implicit val quineIngestConfiguration: Arbitrary[Oss.QuineIngestConfiguration] =
      Arbitrary(Gens.quineIngestConfiguration)
  }
}
