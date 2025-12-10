package com.thatdot.quine.outputs

import cats.data.NonEmptyList
import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.app.v2api.definitions.query.standing.{
  Predicate,
  StandingQueryResultTransformation,
  StandingQueryResultWorkflow,
}
import com.thatdot.quine.v2api.ArbitraryV2ApiCommon

trait ArbitraryStandingQueryOutputs extends ArbitraryV2ApiCommon {

  implicit val genPredicate: Gen[Predicate] = Gen.const(Predicate.OnlyPositiveMatch)

  implicit val arbPredicate: Arbitrary[Predicate] = Arbitrary(genPredicate)

  implicit val genTransformation: Gen[StandingQueryResultTransformation] =
    Gen.const(StandingQueryResultTransformation.InlineData)

  implicit val arbTransformation: Arbitrary[StandingQueryResultTransformation] =
    Arbitrary(genTransformation)

  implicit val genCypherQuery: Gen[QuineDestinationSteps.CypherQuery] = for {
    query <- Gen.alphaNumStr.map(s => s"MATCH (n) WHERE n.id = $$$s RETURN n")
    parameter <- Gen.oneOf("that", "result", "data")
    parallelism <- Gen.chooseNum(1, 16)
    allowAllNodeScan <- Gen.oneOf(true, false)
    shouldRetry <- Gen.oneOf(true, false)
  } yield QuineDestinationSteps.CypherQuery(query, parameter, parallelism, allowAllNodeScan, shouldRetry)

  implicit val arbCypherQuery: Arbitrary[QuineDestinationSteps.CypherQuery] = Arbitrary(genCypherQuery)

  implicit val genFile: Gen[QuineDestinationSteps.File] =
    Gen.alphaNumStr.map(s => QuineDestinationSteps.File(s"/tmp/$s.json"))

  implicit val genHttpEndpoint: Gen[QuineDestinationSteps.HttpEndpoint] = for {
    url <- Gen.alphaNumStr.map(s => s"http://localhost:8080/$s")
    parallelism <- Gen.chooseNum(1, 16)
  } yield QuineDestinationSteps.HttpEndpoint(url, parallelism)

  implicit val genKafka: Gen[QuineDestinationSteps.Kafka] = for {
    topic <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    bootstrapServers <- Gen.alphaNumStr.map(s => s"localhost:9092,$s:9092")
    format <- genOutputFormat
    kafkaProperties <- arbKafkaProperties.arbitrary
  } yield QuineDestinationSteps.Kafka(topic, bootstrapServers, format, kafkaProperties)

  implicit val genKinesis: Gen[QuineDestinationSteps.Kinesis] = for {
    credentials <- arbAwsCredentials.arbitrary
    region <- arbAwsRegion.arbitrary
    streamName <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    format <- genOutputFormat
    parallelism <- Gen.option(Gen.chooseNum(1, 16))
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

  implicit val genReactiveStream: Gen[QuineDestinationSteps.ReactiveStream] = for {
    address <- Gen.oneOf("localhost", "0.0.0.0", "127.0.0.1")
    port <- Gen.chooseNum(1024, 65535)
    format <- genOutputFormat
  } yield QuineDestinationSteps.ReactiveStream(address, port, format)

  implicit val genSNS: Gen[QuineDestinationSteps.SNS] = for {
    credentials <- arbAwsCredentials.arbitrary
    region <- arbAwsRegion.arbitrary
    topic <- Gen.alphaNumStr.map(s => s"arn:aws:sns:us-east-1:123456789:$s")
    format <- genOutputFormat
  } yield QuineDestinationSteps.SNS(credentials, region, topic, format)

  implicit val genSlack: Gen[QuineDestinationSteps.Slack] = for {
    hookUrl <- Gen.const("https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXX")
    onlyPositiveMatchData <- Gen.oneOf(true, false)
    intervalSeconds <- Gen.chooseNum(1, 60)
  } yield QuineDestinationSteps.Slack(hookUrl, onlyPositiveMatchData, intervalSeconds)

  implicit val genQuineDestinationSteps: Gen[QuineDestinationSteps] = Gen.oneOf(
    Gen.const(QuineDestinationSteps.Drop),
    Gen.const(QuineDestinationSteps.StandardOut),
    genFile,
    genHttpEndpoint,
    genKafka,
    genKinesis,
    genReactiveStream,
    genSNS,
    genCypherQuery,
    genSlack,
  )

  implicit val arbQuineDestinationSteps: Arbitrary[QuineDestinationSteps] =
    Arbitrary(genQuineDestinationSteps)

  implicit val genDestinationsList: Gen[NonEmptyList[QuineDestinationSteps]] = for {
    head <- genQuineDestinationSteps
    tailSize <- Gen.chooseNum(0, 3)
    tail <- Gen.listOfN(tailSize, genQuineDestinationSteps)
  } yield NonEmptyList(head, tail)

  implicit val arbDestinationsList: Arbitrary[NonEmptyList[QuineDestinationSteps]] =
    Arbitrary(genDestinationsList)

  implicit val genStandingQueryResultWorkflow: Gen[StandingQueryResultWorkflow] = for {
    name <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    filter <- Gen.option(genPredicate)
    preEnrichmentTransformation <- Gen.option(genTransformation)
    resultEnrichment <- Gen.option(genCypherQuery)
    destinations <- genDestinationsList
  } yield StandingQueryResultWorkflow(
    name,
    filter,
    preEnrichmentTransformation,
    resultEnrichment,
    destinations,
  )

  implicit val arbStandingQueryResultWorkflow: Arbitrary[StandingQueryResultWorkflow] =
    Arbitrary(genStandingQueryResultWorkflow)
}
