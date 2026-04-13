import sbt._

object Dependencies {
  val amazonKinesisClientV = "3.4.2"
  val apacheCommonsCsvV = "1.14.1"
  val avroV = "1.12.1"
  // On update, check whether nettyOverrideV override is removable
  val awsSdkV = "2.42.24"
  // On update, check whether netty-nio-client override in quine-serialization is removable
  val amazonGlueV = "1.1.27"
  val betterMonadicForV = "0.3.1"
  val boopickleV = "1.5.0"
  val bootstrapV = "5.3.6"
  val coreuiV = "5.4.3"
  val coreuiIconsV = "3.0.1"
  val caffeineV = "3.2.3"
  val cassandraClientV = "4.19.2"
  val catsV = "2.13.0"
  val catsEffectV = "3.7.0"
  val circeYamlV = "0.16.1"
  val commonsCodecV = "1.21.0"
  val commonsTextV = "1.15.0"
  val commonsIoV = "2.21.0"
  val dropwizardMetricsV = "4.2.38"
  val embeddedCassandraV = "5.0.3"
  val endpoints4sDefaultV = "1.12.1"
  val endpoints4sCirceV = "2.6.1"
  val endpoints4sHttpServerV = "2.0.1"
  val endpoints4sOpenapiV = "5.0.1"
  val endpoints4sXhrClientV = "5.3.0"
  val flatbuffersV = "25.2.10"
  val graalV = "25.0.2"
  val ioniconsV = "2.0.1"
  val jnrPosixV = "3.1.22"
  val jqueryV = "3.6.3"
  val jwtV = "0.13.0"
  val jwtScalaV = "11.0.4"
  // On update, keep lz4JavaV in sync
  val kafkaClientsV = "3.9.2"
  val kindProjectorV = "0.13.4"
  val logbackV = "1.5.32"
  val laminarV = "17.2.1"
  val waypointV = "10.0.0-M7"
  // Keep in sync with the version kafka-clients (kafkaClientsV) depends on
  val lz4JavaV = "1.10.4"
  // On update, check whether net.jpountz.lz4:lz4 exclusion in quine-mapdb-persistor is removable
  val mapDbV = "3.1.0"
  val metricsInfluxdbV = "1.1.0"
  val msgPackV = "0.9.11"
  val openApiCirceYamlV = "0.11.10"
  val openCypherV = "9.2.3"
  val parboiledV = "1.4.1"
  val pegdownV = "1.6.0"
  val pekkoV = "1.5.0"
  val pekkoTestkitV = "1.5.0"
  val pekkoHttpV = "1.3.0"
  val pekkoHttpCirceV = "3.9.1"
  val pekkoManagementV = "1.2.1"
  val pekkoKafkaV = "1.1.0"
  val pekkoConnectorsV = "1.3.0"
  val plotlyV = "2.25.2"
  val pprintV = "0.9.6"
  val protobufV = "4.34.1"
  val protobufCommonV = "2.14.2"
  val pureconfigV = "0.17.10"
  val antlr4RuntimeV = "4.13.2"
  val lsp4jV = "0.24.0"
  val guavaV = "33.3.0-jre"
  val memeid4sV = "0.8.0"
  val munitV = "1.2.4"
  val quineCommonV = "0.0.4"
  val reactV = "17.0.2"
  val rocksdbV = "10.10.1"
  val scaffeineV = "5.3.0"
  val scalaCheckV = "1.19.0"
  val scalaJavaTimeV = "2.6.0"
  val scalaLoggingV = "3.9.6"
  val scalaParserCombinatorsV = "2.4.0"
  val scalaTestScalaCheckV = "3.2.18.0"
  val scalajsDomV = "2.8.1"
  val scalaTestV = "3.2.20"
  val scalajsMacroTaskExecutorV = "1.1.1"
  val scoptV = "4.1.0"
  val shapelessV = "2.3.13"
  val ayzaV = "10.0.4"
  // On update, check whether com.datastax.oss exclusion in quine-cassandra-persistor is removable
  val sigv4AuthCassandraPluginV = "4.0.9"
  // On update, check whether any NPM Override Versions (below) are removable
  val stoplightElementsV = "9.0.1"
  val sugarV = "2.0.6"
  val tapirV = "1.13.15"
  val ujsonCirceV = "3.3.1"
  val circeV = "0.14.15"
  val circeGenericExtrasV = "0.14.4"
  val circeOpticsV = "0.15.1"
  val webjarsLocatorV = "0.52"

  // === Vis-Network and Peer Dependencies
  val visNetworkV = "10.0.2"
  val visDataV = "8.0.3"
  val visUtilV = "6.0.0"
  val egjsHammerjsV = "2.0.17"
  val componentEmitterV = "2.0.0"
  val keycharmV = "0.4.0"
  val uuidV = "11.1.0"

  // === JVM Override Versions ===
  // == Remove overrides when parents require fixed versions of the transitive dependency. ==

  // Parent: AWS SDK (awsSdkV) via transitive Netty dependency
  val nettyOverrideV = "4.1.132.Final" // CVE-2026-33871

  val jvmDependencyOverrides: Seq[ModuleID] = Seq(
    "io.netty" % "netty-handler" % nettyOverrideV,
    "io.netty" % "netty-codec-http" % nettyOverrideV,
    "io.netty" % "netty-codec-http2" % nettyOverrideV,
    "io.netty" % "netty-transport-classes-epoll" % nettyOverrideV,
  )

  // === NPM Override Versions ===
  // == Remove overrides when parents require fixed versions of the transitive dependency. ==

  // Parents: @stoplight/elements (stoplightElementsV), webpack (scalajs-bundler)
  val lodashV = "4.18.0" // CVE-2025-13465 (GHSA-xxjr-mmjv-4gpg), CVE-2026-4800

  // Parent: @stoplight/elements (stoplightElementsV) via react-router-dom
  val reactRouterV = "6.30.3" // CVE-2025-68470 & CVE-2026-22029 (GHSA-2w69-qvjg-hvjx)
  val remixRunRouterV = "1.23.2" // CVE-2026-22029 (GHSA-2w69-qvjg-hvjx)

  // Parents: @stoplight/elements (stoplightElementsV), glob.
  val minimatchV = "3.1.5" // CVE-2026-27903 & CVE-2026-27904

  // Parent: @stoplight/elements (stoplightElementsV) via @stoplight/yaml and openapi3-ts
  val yamlV = "1.10.3" // CVE-2026-33532 (GHSA-48c2-rrv3-qjmp)

  // Parent: @stoplight/elements (stoplightElementsV) via minimatch
  val braceExpansionV = "1.1.13" // CVE-2026-33750 (GHSA-f886-m6hf-6m8v)
}
