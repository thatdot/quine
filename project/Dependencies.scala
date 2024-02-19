object Dependencies {
  val pekkoV = "1.0.1"
  val pekkoHttpV = "1.0.0"
  val pekkoStreamV = "1.0.1"
  val pekkoKafkaV = "1.0.0"
  val pekkoConnectorsV = "1.0.1"
  val amazonKinesisClientV = "2.5.4"
  val amazonGlueV = "1.1.18"
  val awsSdkV = "2.20.159"
  // Required for amazon-kinesis-client
  val awsSdkv1V = "1.12.647"
  val betterMonadicForV = "0.3.1"
  val bootstrapV = "5.3.0"
  val cassandraClientV = "4.15.0"
  val catsV = "2.10.0"
  val catsEffectV = "3.5.3"
  val commonsCodecV = "1.15"
  val commonsTextV = "1.11.0"
  val commonsIoV = "2.15.1"
  val diffxV = "0.9.0"
  val dropwizardMetricsV = "4.2.25"
  val embeddedCassandraV = "4.1.0"
  val endpoints4sDefaultV = "1.11.1"
  val endpoints4sHttpServerV = "1.0.1"
  val endpoints4sOpenapiV = "4.5.1"
  val endpoints4sXhrClientV = "5.3.0"
  val flatbuffersV = "23.5.26"
  val guavaV = "33.0.0-jre"
  val ioniconsV = "2.0.1"
  val jnrPosixV = "3.1.18"
  val jqueryV = "3.6.3"
  val jwtScalaV = "10.0.0"
  // pekko-connectors 1.0.1 requires 3.0.1, which is vulnerable to CVE-2022-34917
  val kafkaClientsV = "3.6.1"
  // kotlin-stdlib version explicitly specified is as close as possible to the version included in mapdb
  // 3.0.10 [[mapDbV]], but with a fix for CVE-2022-24329
  val kotlinStdlibV = "1.9.22"
  val logbackConfigV = "0.4.0"
  val logbackV = "1.4.14"
  val logstashLogbackV = "7.4"
  val lz4JavaV = "1.8.0" // Try to keep this in sync w/ the version kafka-client depends on.
  val mapDbV = "3.0.10"
  val memeIdV = "0.8.0"
  val metricsInfluxdbV = "1.1.0"
  val msgPackV = "0.9.8"
  val openCypherV = "9.1.1"
  val parboiledV = "1.4.1"
  val pegdownV = "1.6.0"
  val plotlyV = "1.57.1"
  val protobufV = "3.25.2"
  val protobufCommonV = "2.14.2"
  val pureconfigV = "0.17.5"
  val reactPlotlyV = "2.5.1"
  val reactV = "17.0.2"
  val rocksdbV = "8.5.4"
  val scalaCheckV = "1.17.0"
  val scalaCollectionCompatV = "2.11.0"
  val scalaJava8CompatV = "1.0.2"
  val scalaJavaTimeV = "2.5.0"
  val scalaLoggingV = "3.9.5"
  val scalaParserCombinatorsV = "2.3.0"
  val scalaTestScalaCheckV = "3.2.17.0"
  val scalajsDomV = "2.6.0"
  val scalaTestV = "3.2.17"
  val scalajsMacroTaskExecutorV = "1.1.1"
  val scoptV = "4.1.0"
  val shapelessV = "2.3.10"
  val slinkyV = "0.7.3"
  val snakeYamlV = "2.7"
  // pekko-connectors-kafka 1.0.0 depends on an older but compatible version with vulnerabilities CVE-2023-34453,
  // CVE-2023-34454, CVE-2023-34455. Pin this until the indirect dependency version is changed to not have these
  // vulnerabilities.
  val snappyV = "1.1.10.5"
  val sugarV = "2.0.6"
  val stoplightElementsV = "7.12.0"
  val ujsonV = "1.6.0"
  val circeV = "0.14.6"
  val circeOpticsV = "0.15.0"
  val visNetworkV = "8.2.0"
  val webjarsLocatorV = "0.50"
}
