package com.thatdot.quine.outputs

import cats.data.NonEmptyList
import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.app.v2api.definitions.query.standing.{
  Predicate,
  StandingQueryResultTransformation,
  StandingQueryResultWorkflow,
}
import com.thatdot.quine.v2api.V2ApiCommonGenerators
import com.thatdot.quine.{AwsGenerators, ScalaPrimitiveGenerators}

object StandingQueryOutputGenerators {

  import AwsGenerators.Gens._
  import ScalaPrimitiveGenerators.Gens._
  import V2ApiCommonGenerators.Gens._

  object Gens {
    val predicate: Gen[Predicate] = Gen.const(Predicate.OnlyPositiveMatch)

    val transformation: Gen[StandingQueryResultTransformation] =
      Gen.const(StandingQueryResultTransformation.InlineData)

    val cypherQuery: Gen[QuineDestinationSteps.CypherQuery] = for {
      query <- nonEmptyAlphaNumStr.map(s => s"MATCH (n) WHERE n.id = $$$s RETURN n")
      parameter <- Gen.oneOf("that", "result", "data")
      parallelism <- numWithinBits(4)
      allowAllNodeScan <- bool
      shouldRetry <- bool
    } yield QuineDestinationSteps.CypherQuery(query, parameter, parallelism, allowAllNodeScan, shouldRetry)

    val file: Gen[QuineDestinationSteps.File] =
      nonEmptyAlphaNumStr.map(s => QuineDestinationSteps.File(s"/tmp/$s.json"))

    val httpEndpoint: Gen[QuineDestinationSteps.HttpEndpoint] = for {
      url <- nonEmptyAlphaNumStr.map(s => s"http://localhost:8080/$s")
      parallelism <- numWithinBits(4)
    } yield QuineDestinationSteps.HttpEndpoint(url, parallelism)

    val kafka: Gen[QuineDestinationSteps.Kafka] = for {
      topic <- nonEmptyAlphaNumStr
      bootstrapServers <- nonEmptyAlphaNumStr.map(s => s"localhost:9092,$s:9092")
      format <- outputFormat
      props <- kafkaProperties
    } yield QuineDestinationSteps.Kafka(topic, bootstrapServers, format, props)

    val kinesis: Gen[QuineDestinationSteps.Kinesis] = for {
      credentials <- optAwsCredentials
      region <- optAwsRegion
      streamName <- nonEmptyAlphaNumStr
      format <- outputFormat
      parallelism <- Gen.option(numWithinBits(4))
      maxBatchSize <- Gen.option(Gen.chooseNum(1, 500))
      maxRecordsPerSecond <- Gen.option(Gen.chooseNum(100, 10000))
      maxBytesPerSecond <- Gen.option(Gen.chooseNum(1000, 1000000))
    } yield QuineDestinationSteps.Kinesis(
      credentials,
      region,
      streamName,
      format,
      parallelism,
      maxBatchSize,
      maxRecordsPerSecond,
      maxBytesPerSecond,
    )

    val reactiveStream: Gen[QuineDestinationSteps.ReactiveStream] = for {
      address <- Gen.oneOf("localhost", "0.0.0.0", "127.0.0.1")
      port <- Gen.chooseNum(1024, 65535)
      format <- outputFormat
    } yield QuineDestinationSteps.ReactiveStream(address, port, format)

    val sns: Gen[QuineDestinationSteps.SNS] = for {
      credentials <- optAwsCredentials
      region <- optAwsRegion
      topic <- nonEmptyAlphaNumStr.map(s => s"arn:aws:sns:us-east-1:123456789:$s")
      format <- outputFormat
    } yield QuineDestinationSteps.SNS(credentials, region, topic, format)

    val slack: Gen[QuineDestinationSteps.Slack] = for {
      hookUrl <- Gen.const("https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXX")
      onlyPositiveMatchData <- bool
      intervalSeconds <- Gen.chooseNum(1, 60)
    } yield QuineDestinationSteps.Slack(hookUrl, onlyPositiveMatchData, intervalSeconds)

    val quineDestinationSteps: Gen[QuineDestinationSteps] = Gen.oneOf(
      Gen.const(QuineDestinationSteps.Drop),
      Gen.const(QuineDestinationSteps.StandardOut),
      file,
      httpEndpoint,
      kafka,
      kinesis,
      reactiveStream,
      sns,
      cypherQuery,
      slack,
    )

    val destinationsList: Gen[NonEmptyList[QuineDestinationSteps]] = for {
      head <- quineDestinationSteps
      tailSize <- smallPosNum
      tail <- Gen.listOfN(tailSize, quineDestinationSteps)
    } yield NonEmptyList(head, tail)

    val standingQueryResultWorkflow: Gen[StandingQueryResultWorkflow] = for {
      name <- nonEmptyAlphaNumStr
      filter <- Gen.option(predicate)
      preEnrichmentTransformation <- Gen.option(transformation)
      resultEnrichment <- Gen.option(cypherQuery)
      destinations <- destinationsList
    } yield StandingQueryResultWorkflow(
      name,
      filter,
      preEnrichmentTransformation,
      resultEnrichment,
      destinations,
    )
  }

  object Arbs {
    implicit val predicate: Arbitrary[Predicate] = Arbitrary(Gens.predicate)
    implicit val transformation: Arbitrary[StandingQueryResultTransformation] = Arbitrary(Gens.transformation)
    implicit val cypherQuery: Arbitrary[QuineDestinationSteps.CypherQuery] = Arbitrary(Gens.cypherQuery)
    implicit val file: Arbitrary[QuineDestinationSteps.File] = Arbitrary(Gens.file)
    implicit val httpEndpoint: Arbitrary[QuineDestinationSteps.HttpEndpoint] = Arbitrary(Gens.httpEndpoint)
    implicit val kafka: Arbitrary[QuineDestinationSteps.Kafka] = Arbitrary(Gens.kafka)
    implicit val kinesis: Arbitrary[QuineDestinationSteps.Kinesis] = Arbitrary(Gens.kinesis)
    implicit val reactiveStream: Arbitrary[QuineDestinationSteps.ReactiveStream] = Arbitrary(Gens.reactiveStream)
    implicit val sns: Arbitrary[QuineDestinationSteps.SNS] = Arbitrary(Gens.sns)
    implicit val slack: Arbitrary[QuineDestinationSteps.Slack] = Arbitrary(Gens.slack)
    implicit val quineDestinationSteps: Arbitrary[QuineDestinationSteps] = Arbitrary(Gens.quineDestinationSteps)
    implicit val destinationsList: Arbitrary[NonEmptyList[QuineDestinationSteps]] = Arbitrary(Gens.destinationsList)
    implicit val standingQueryResultWorkflow: Arbitrary[StandingQueryResultWorkflow] =
      Arbitrary(Gens.standingQueryResultWorkflow)
  }
}
