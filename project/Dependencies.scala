import sbt._
import scalajsbundler.util.JSON
import scalajsbundler.util.JSON.{obj, str}

object Dependencies {
  // On update, check whether opentelemetryOverrideV is removable
  val amazonKinesisClientV = "3.5.1"
  val apacheCommonsCsvV = "1.14.1"
  val avroV = "1.12.1"
  // On update, check whether jacksonOverrideV override is removable
  val awsSdkV = "2.49.5"
  // On update, check whether nettyOverrideV or wireOverrideV are removable
  val amazonGlueV = "1.1.27"
  val betterMonadicForV = "0.3.1"
  val boopickleV = "1.5.0"
  val bootstrapV = "5.3.6"
  val coreuiV = "5.4.3"
  val d3V = "7.9.0"
  val coreuiIconsV = "3.0.1"
  val fontsourceInterV = "5.2.8"
  val fontsourceJetBrainsMonoV = "5.2.8"
  val caffeineV = "3.2.4"
  // On update check whether hdrhistographOverrideV is removable
  val cassandraClientV = "4.19.3"
  val catsV = "2.13.0"
  val catsEffectV = "3.7.0"
  val circeYamlV = "0.16.1"
  val commonsCodecV = "1.22.1"
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
  val logbackV = "1.6.1"
  val laminarV = "17.2.1"
  val waypointV = "10.0.0-M7"
  // Keep in sync with the version kafka-clients (kafkaClientsV) depends on
  val lz4JavaV = "1.11.1"
  // On update, check whether net.jpountz.lz4:lz4 exclusion in quine-mapdb-persistor is removable
  val mapDbV = "3.1.0"
  val metricsInfluxdbV = "1.1.0"
  val msgPackV = "0.9.12"
  val openApiCirceYamlV = "0.11.10"
  val openCypherV = "9.2.3"
  val parquet4sCoreV = "2.23.0"
  val deltaKernelV = "4.0.0"
  // 3.4.3 fixes the native HDFS client CVE-2025-27821 and ships patched commons-lang3 3.18.0
  // (so no commons-lang3 suppression is needed). We pull Hadoop only so parquet4s can read local
  // Parquet files; HDFS and the native client are never exercised.
  val hadoopV = "3.5.0"
  // parquet4s-core pulls aircompressor 2.0.2, which is vulnerable to CVE-2025-67721. Fixed in 2.0.3.
  // Remove this override once parquet4s upgrades the transitive.
  val aircompressorV = "2.0.3"
  val parboiledV = "1.4.1"
  val pegdownV = "1.6.0"
  val pekkoV = "1.6.0"
  val pekkoTestkitV = "1.6.0"
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
  // Code-completion candidate collection on ANTLR parser ATNs; its ANTLR version must stay
  // in lockstep with antlr4RuntimeV (antlr4-c3-java 1.2.0 is built against ANTLR 4.13.2).
  val antlr4C3V = "1.2.0"
  val lsp4jV = "0.24.0"
  val guavaV = "33.3.0-jre"
  val memeid4sV = "0.8.0"
  val munitV = "1.3.0"
  val quineCommonV = "0.0.4"
  val reactV = "17.0.2"
  val rocksdbV = "10.10.1.1"
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
  val ayzaV = "10.0.6"
  // On update, check whether com.datastax.oss exclusion in quine-cassandra-persistor is removable
  val sigv4AuthCassandraPluginV = "4.0.9"
  // On update, check whether any NPM Override Versions (below) are removable
  val stoplightElementsV = "9.0.24"
  val sugarV = "2.0.6"
  val tapirV = "1.13.30"
  val ujsonCirceV = "3.3.1"
  val circeV = "0.14.15"
  val circeGenericExtrasV = "0.14.4"
  val circeOpticsV = "0.15.1"
  val webjarsLocatorV = "0.52"

  // === Frontend Build Tooling ===
  // Overrides scalajs-bundler 0.21.1's default of webpack 5.24.3. Webpack ≥5.75.0 is required to
  // correctly bundle packages that use ES2022 class static initialization blocks (e.g.
  // monaco-editor ≥0.53): earlier versions fail to rewrite imported bindings referenced inside
  // static blocks, producing bundles that throw ReferenceError at runtime despite a green build.
  val webpackV = "5.107.2"

  // === Query Editor (Monaco) ===
  // The @thatdot/query-editor package lives in-tree at public/query-editor; its TypeScript
  // source is compiled directly by each browser module's webpack (resolved via a ts-loader
  // alias in common.webpack.config.js), so it is not an npm dependency.
  // monaco-editor is that package's peer dependency and must be pinned EXACTLY, in lockstep
  // with the version the package targets: Monaco breaks APIs in 0.x minors and the package
  // deep-imports unstable internal paths. yarn 1 does not auto-install peer dependencies, so
  // the pin lives here in each consuming browser module.
  val monacoEditorV = "0.56.0"

  // zod is a runtime dependency of the in-tree query editor package (its JSON-RPC / LSP payload
  // parsing). Pinned in lockstep with public/query-editor/package.json, like monaco-editor above.
  val zodV = "3.25.76"

  // === Vis-Network and Peer Dependencies
  val visNetworkV = "10.0.2"
  val visDataV = "8.0.3"
  val visUtilV = "6.0.0"
  val egjsHammerjsV = "2.0.17"
  val componentEmitterV = "2.0.0"
  val keycharmV = "0.4.0"
  val uuidV = "14.0.0"

  // === JVM Override Versions ===
  // == Remove overrides when parents require fixed versions of the transitive dependency. ==

  /** Parent: [[awsSdkV]] */
  val jacksonOverrideV = "2.22.1"

  /** Parent: [[amazonGlueV]] */
  val wireOverrideV = "6.4.5"

  val okhttpOverrideV = "5.3.2"

  /** Parent: [[amazonKinesisClientV]] */
  val opentelemetryOverrideV = "1.62.0"

  /** Parent: [[cassandraClientV]] */
  val hdrhistographOverrideV = "2.2.2"

  // Parent: AWS SDK (awsSdkV). The AWS SDK is often slow to update its dependencies, and CVE reports for netty have
  // been frequent. So although this netty override is currently unnecessary, we'll keep it commented-out in the code.
//  val nettyOverrideV = "4.1.135.Final"

  val jvmDependencyOverrides: Seq[ModuleID] = Seq(
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonOverrideV,
    // Overriding only wire-compiler and wire-schema should be sufficient to transitively override
    // the rest of the wire dependencies, but wire-runtime and wire-runtime-jvm are the specific
    // projects with CVEs, so we'll list them too just to be explicit.
    "com.squareup.wire" % "wire-compiler" % wireOverrideV,
    "com.squareup.wire" % "wire-schema" % wireOverrideV,
    "com.squareup.wire" % "wire-runtime" % wireOverrideV,
    "com.squareup.wire" % "wire-runtime-jvm" % wireOverrideV,
    "com.squareup.okhttp3" % "okhttp" % okhttpOverrideV,
    "io.opentelemetry" % "opentelemetry-api" % opentelemetryOverrideV,
    "org.hdrhistogram" % "HdrHistogram" % hdrhistographOverrideV,
//    "io.netty" % "netty-handler" % nettyOverrideV,
//    "io.netty" % "netty-codec-http" % nettyOverrideV,
//    "io.netty" % "netty-codec-http2" % nettyOverrideV,
//    "io.netty" % "netty-transport-classes-epoll" % nettyOverrideV,
  )

  // === NPM Override Versions ===
  // == Remove overrides when parents require fixed versions of the transitive dependency. ==

  /** Parent: minimatch [[minimatchV]] */
  val braceExpansionV = "5.0.8" // CVE-2026-14257

  /** Parent: monaco-editor [[monacoEditorV]] */
  val dompurifyV = "3.4.13" // CVE-2026-65901

  /** Parent: @stoplight/elements [[stoplightElementsV]] via react-use */
  val jsCookieV = "3.0.8" // CVE-2026-46625 (GHSA-qjx8-664m-686j)

  /** Parents: [[stoplightElementsV]], [[webpackV]] */
  val lodashV = "4.18.1"

  /** Parents: @stoplight/elements (stoplightElementsV), glob. */
  val minimatchV = "5.1.9" // CVE-2026-27903 & CVE-2026-27904

  /** Parent: @stoplight/elements [[stoplightElementsV]] */
  val reactRouterDomV = "7.13.0" // CVE-2026-53668

  /** Parent: @stoplight/elements [[stoplightElementsV]] via @stoplight/yaml and openapi3-ts */
  val yamlV = "1.10.3" // CVE-2026-33532 (GHSA-48c2-rrv3-qjmp)

  // The yarn `resolutions` object forcing the patched versions above, shared by every browser
  // project's `Compile / additionalNpmConfig`. (Not scalajs-bundler's `npmResolutions` key, which
  // settles conflicting npmDependencies declarations rather than overriding transitive versions.)
  val yarnResolutions: JSON = obj(
    "brace-expansion" -> str(braceExpansionV),
    "dompurify" -> str(dompurifyV),
    "js-cookie" -> str(jsCookieV),
    "lodash" -> str(lodashV),
    "minimatch" -> str(minimatchV),
    "react-router-dom" -> str(reactRouterDomV),
    "yaml" -> str(yamlV),
  )
}
