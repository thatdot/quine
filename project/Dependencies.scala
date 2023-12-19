object Dependencies {

  val akkaHttpV = "10.2.10"
  val akkaV = "2.6.20"
  val alpakkaCsvV = "3.0.4"
  val alpakkaKafkaV = "2.1.1"
  val alpakkaKinesisV = "3.0.4"
  val alpakkaS3V = "3.0.4"
  val alpakkaSQSV = "3.0.4"
  val alpakkaSnsV = "3.0.4"
  val alpakkaSseV = "3.0.4"
  val alpakkaTextV = "3.0.4"
  // avro version explicitly specified is as close as possible to the version included in kcl-akka-stream 5.0.0
  // [[kclAkkaV]], and alpakka-kinesis 3.0.4 [[alpakkaKinesisV]], but with a fix for CVE-2023-39410
  val avroV = "1.11.3"
  val awsSdkV = "2.20.159"
  val betterMonadicForV = "0.3.1"
  val bootstrapV = "5.3.0"
  val cassandraClientV = "4.15.0"
  val catsV = "2.10.0"
  val catsEffectV = "3.5.2"
  val commonsCodecV = "1.15"
  // commons-compress version explicitly specified is as close as possible to the version included in kcl-akka-stream
  // 5.0.0 [[kclAkkaV]] and alpakka-kinesis 3.0.4 [[alpakkaKinesisV]], but with fixes for CVE-2021-35515,
  // CVE-2021-35516, CVE-2021-35517, CVE-2021-36090
  val commonsCompressV = "1.25.0"
  val commonsTextV = "1.10.0"
  val commonsIoV = "2.15.1"
  val dropwizardMetricsV = "4.2.20"
  val embeddedCassandraV = "4.0.7"
  val endpoints4sDefaultV = "1.9.0"
  val endpoints4sHttpServerV = "7.1.0"
  val endpoints4sOpenapiV = "4.3.0"
  val endpoints4sXhrClientV = "5.2.0"
  val flatbuffersV = "23.5.26"
  val guavaV = "32.1.3-jre"
  val ioniconsV = "2.0.1"
  // jackson version explicitly specified is as close as possible to the version included in kcl-akka-stream 5.0.0
  // [[kclAkkaV]], but with a fix for CVE-2020-28491
  val jacksonCborV = "2.15.2"
  val jnrPosixV = "3.1.18"
  val jqueryV = "3.6.3"
  // kafka-clients version explicitly specified is as close as possible to the version included in akka-stream-kafka
  // 2.1.1 [[alpakkaKafkaV]], kcl-akka-stream 5.0.0 [[kclAkkaV]], and alpakka-kinesis 3.0.4 [[alpakkaKinesisV]],
  // but with a fix for CVE-2021-38153
  val kafkaClientsV = "2.7.2"
  val kclAkkaV = "5.0.0"
  // kotlin-stdlib version explicitly specified is as close as possible to the version included in mapdb
  // 3.0.10 [[mapDbV]], but with a fix for CVE-2022-24329
  val kotlinStdlibV = "1.9.10"
  val logbackConfigV = "0.4.0"
  val logbackV = "1.4.13"
  val logstashLogbackV = "7.4"
  val lz4JavaV = "1.8.0" // Try to keep this in sync w/ the version kafka-client depends on.
  val mapDbV = "3.0.10"
  val memeIdV = "0.8.0"
  val metricsInfluxdbV = "1.1.0"
  val msgPackV = "0.9.6"
  val openCypherV = "9.0.20210312"
  val parboiledV = "1.4.1"
  val pegdownV = "1.6.0"
  val plotlyV = "1.57.1"
  val protobufV = "3.21.10"
  val protobufCommonV = "2.14.2"
  val pureconfigV = "0.17.4"
  val reactPlotlyV = "2.5.1"
  val reactV = "17.0.2"
  val rocksdbV = "7.10.2"
  val scalaCheckV = "1.17.0"
  val scalaCollectionCompatV = "2.11.0"
  val scalaJavaTimeV = "2.5.0"
  val scalaLoggingV = "3.9.5"
  // scalaParserCombinatorsV could be at 2.1.1 (or higher), but doing so conflicts with akka 2.6. Due to licensing
  // concerns, we cannot further upgrade akkaV, so we cannot easily upgrade scalaParserCombinatorsV
  val scalaParserCombinatorsV = "1.1.2" // scala-steward:off
  val scalaTestScalaCheckV = "3.2.17.0"
  val scalajsDomV = "2.6.0"
  val scalaTestV = "3.2.17"
  val scalajsMacroTaskExecutorV = "1.1.1"
  val scoptV = "4.1.0"
  val shapelessV = "2.3.10"
  val slinkyV = "0.7.3"
  val snakeYamlV = "2.7"
  // snappy version explicitly specified is binary-compatible with the versions included in alpakka-kafka 2.1.1
  // [[alpakkaKafkaV]] and kcl-akka-stream 5.0.0 [[kclAkkaV]], but with fixes for CVE-2023-34453, CVE-2023-34454,
  // CVE-2023-34455
  val snappyV = "1.1.10.5"
  val sugarV = "2.0.6"
  val stoplightElementsV = "7.12.0"
  val ujsonV = "1.6.0"
  val circeV = "0.14.6"
  val circeOpticsV = "0.14.1"
  val visNetworkV = "8.2.0"
  val webjarsLocatorV = "0.47"
}
