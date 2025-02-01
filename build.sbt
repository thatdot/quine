import QuineSettings.*
import Dependencies.*

ThisBuild / resolvers += "thatDot maven" at "https://s3.us-west-2.amazonaws.com/com.thatdot.dependencies/release/"

ThisBuild / scalaVersion := scalaV

addCommandAlias("fmtall", "; scalafmtAll; scalafmtSbt")
addCommandAlias("fixall", "; scalafixAll; fmtall")

ThisBuild / evictionErrorLevel := Level.Info

Global / concurrentRestrictions := Seq(
  Tags.limit(Tags.Test, 1),
)

// Core streaming graph interpreter
lazy val `quine-core`: Project = project
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % shapelessV,
      "org.apache.pekko" %% "pekko-actor" % pekkoV,
      "org.apache.pekko" %% "pekko-stream" % pekkoStreamV,
      "org.apache.pekko" %% "pekko-slf4j" % pekkoV,
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingV,
      "io.dropwizard.metrics" % "metrics-core" % dropwizardMetricsV,
      "io.circe" %% "circe-parser" % circeV,
      "org.msgpack" % "msgpack-core" % msgPackV,
      "org.apache.commons" % "commons-text" % commonsTextV,
      "com.github.blemale" %% "scaffeine" % scaffeineV,
      "io.github.hakky54" % "sslcontext-kickstart" % sslContextKickstartV,
      // Testing
      "org.scalatest" %% "scalatest" % scalaTestV % Test,
      "org.scalacheck" %% "scalacheck" % scalaCheckV % Test,
      "org.scalatestplus" %% "scalacheck-1-17" % scalaTestScalaCheckV % Test,
      "ch.qos.logback" % "logback-classic" % logbackV % Test,
      "commons-io" % "commons-io" % commonsIoV % Test,
      "org.typelevel" %% "cats-core" % catsV,
      "org.typelevel" %% "cats-effect" % catsEffectV,
      "com.thatdot" %% "query-language" % quineQueryV,
      "com.thatdot" %% "quine-id" % quineCommonV,
      "com.lihaoyi" %% "pprint" % pprintV,
    ),
    // Compile different files depending on scala version
    Compile / unmanagedSourceDirectories += {
      (Compile / sourceDirectory).value / "scala-2.13"
    },
    addCompilerPlugin("org.typelevel" %% "kind-projector" % kindProjectorV cross CrossVersion.full),
    // Uncomment the following 2 lines to generate flamegraphs for the project's compilation in target/scala-2.13/classes/META-INF
    // (look for `.flamegraph` files -- these may be imported into intellij profiler or flamegraph.pl)
    // ThisBuild / scalacOptions += "-Vstatistics",
    // addCompilerPlugin("ch.epfl.scala" %% "scalac-profiling" % "1.1.0-RC3" cross CrossVersion.full)
  )
  .enablePlugins(BuildInfoPlugin, FlatcPlugin)
  .settings(
    // Allow BuildInfo to be cached on `-DIRTY` versions, to avoid recompilation during development
    buildInfoOptions := (if (git.gitUncommittedChanges.value) Seq() else Seq(BuildInfoOption.BuildTime)),
    buildInfoKeys := Seq[BuildInfoKey](
      version,
      git.gitHeadCommit,
      git.gitUncommittedChanges,
      git.gitHeadCommitDate,
      BuildInfoKey.action("javaVmName")(scala.util.Properties.javaVmName),
      BuildInfoKey.action("javaVendor")(scala.util.Properties.javaVendor),
      BuildInfoKey.action("javaVersion")(scala.util.Properties.javaVersion),
    ),
    buildInfoPackage := "com.thatdot.quine",
  )

// MapDB implementation of a Quine persistor
lazy val `quine-mapdb-persistor`: Project = project
  .settings(commonSettings)
  .dependsOn(`quine-core` % "compile->compile;test->test")
  .settings(
    /* `net.jpountz.lz4:lz4` was moved to `org.lz4:lz4-java`, but MapDB hasn't
     * adapted to this change quickly. However, since other parts of the Java
     * ecosystem _have_ (example: `pekko-connectors-kafka`), we need to exclude the
     * bad JAR and explicitly pull in the good one.
     */
    libraryDependencies ++= Seq(
      ("org.mapdb" % "mapdb" % mapDbV).exclude("net.jpountz.lz4", "lz4"),
      "org.lz4" % "lz4-java" % lz4JavaV,
    ),
  )

// RocksDB implementation of a Quine persistor
lazy val `quine-rocksdb-persistor`: Project = project
  .settings(commonSettings)
  .dependsOn(`quine-core` % "compile->compile;test->test")
  .settings(
    libraryDependencies ++= Seq(
      "org.rocksdb" % "rocksdbjni" % rocksdbV,
    ),
  )

// Cassandra implementation of a Quine persistor
lazy val `quine-cassandra-persistor`: Project = project
  .configs(Integration)
  .settings(commonSettings, integrationSettings)
  .dependsOn(`quine-core` % "compile->compile;test->test")
  .enablePlugins(spray.boilerplate.BoilerplatePlugin)
  .settings(
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % catsV,
      "org.apache.cassandra" % "java-driver-query-builder" % cassandraClientV,
      // The org name for the Cassandra java-driver was changed from com.datastax.oss to org.apache.cassandra
      // The sigv4-auth plugin specifies a dep on com.datastax.oss, SBT doesn't know that our org.apache.cassandra
      // dep is supposed to be the replacement for that, and includes both on the classpath, which then conflict
      // at the sbt-assembly step (because they both have the same package names internally).
      "software.aws.mcs" % "aws-sigv4-auth-cassandra-java-driver-plugin" % "4.0.9" exclude ("com.datastax.oss", "java-driver-core"),
      "software.amazon.awssdk" % "sts" % awsSdkV,
      "com.github.nosan" % "embedded-cassandra" % embeddedCassandraV % Test,
    ),
  )

// Parser and interepreter for a subset of [Gremlin](https://tinkerpop.apache.org/gremlin.html)
lazy val `quine-gremlin`: Project = project
  .settings(commonSettings)
  .dependsOn(`quine-core` % "compile->compile;test->test")
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-parser-combinators" % scalaParserCombinatorsV,
      "org.apache.commons" % "commons-text" % commonsTextV,
      "org.scalatest" %% "scalatest" % scalaTestV % Test,
    ),
  )

// Compiler for compiling [Cypher](https://neo4j.com/docs/cypher-manual/current/) into Quine queries
lazy val `quine-cypher`: Project = project
  .settings(commonSettings)
  .dependsOn(`quine-core` % "compile->compile;test->test")
  .settings(
    scalacOptions ++= Seq(
      "-language:reflectiveCalls",
      "-Xlog-implicits",
    ),
    libraryDependencies ++= Seq(
      "com.thatdot.opencypher" %% "expressions" % openCypherV,
      "com.thatdot.opencypher" %% "front-end" % openCypherV,
      "com.thatdot.opencypher" %% "opencypher-cypher-ast-factory" % openCypherV,
      "com.thatdot.opencypher" %% "util" % openCypherV,
      "commons-codec" % "commons-codec" % commonsCodecV,
      "org.typelevel" %% "cats-core" % catsV,
      "org.scalatest" %% "scalatest" % scalaTestV % Test,
      "org.apache.pekko" %% "pekko-stream-testkit" % pekkoV % Test,
    ),
    addCompilerPlugin("org.typelevel" % "kind-projector" % kindProjectorV cross CrossVersion.full),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % betterMonadicForV),
  )

/*
 * Version 7.5.1. It is expected that `Network` and `DataSet` are available under
 * A globally available `vis` object, as with
 *
 * ```html
 * <script
 *   type="text/javascript"
 *   src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"
 * ></script>
 * ```
 *
 * Thanks to [`scala-js-ts-importer`][ts-importer] which made it possible to generate
 * A first pass of the facade directly from the Typescipt bindings provided with
 * `vis-network` (see `Network.d.ts`).
 *
 * [ts-importer]: https://github.com/sjrd/scala-js-ts-importer
 * [visjs]: https://github.com/visjs/vis-network
 */
lazy val `visnetwork-facade`: Project = project
  .settings(commonSettings)
  .enablePlugins(ScalaJSPlugin)
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-js" %%% "scalajs-dom" % scalajsDomV,
    ),
  )

// REST API specifications for `quine`-based applications
lazy val `quine-endpoints` = crossProject(JSPlatform, JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("quine-endpoints"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.endpoints4s" %%% "json-schema-generic" % endpoints4sDefaultV,
      "org.endpoints4s" %%% "json-schema-circe" % "2.6.1",
      "io.circe" %% "circe-core" % circeV,
      "org.endpoints4s" %%% "openapi" % endpoints4sOpenapiV,
      "com.lihaoyi" %% "ujson-circe" % ujsonCirceV, // For the OpenAPI rendering
      "org.scalacheck" %%% "scalacheck" % scalaCheckV % Test,
      "com.softwaremill.sttp.tapir" %% "tapir-core" % tapirV, // For tapir annotations
    ),
  )
  .jsSettings(
    // Provides an implementatAllows us to use java.time.Instant in Scala.js
    libraryDependencies += "io.github.cquiroz" %%% "scala-java-time" % scalaJavaTimeV,
  )

// Quine web application
lazy val `quine-browser`: Project = project
  .settings(commonSettings, slinkySettings)
  .dependsOn(`quine-endpoints`.js, `visnetwork-facade`)
  .enablePlugins(ScalaJSBundlerPlugin)
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-js" %%% "scalajs-dom" % scalajsDomV,
      "org.scala-js" %%% "scala-js-macrotask-executor" % scalajsMacroTaskExecutorV,
      "org.endpoints4s" %%% "xhr-client" % endpoints4sXhrClientV,
    ),
    Compile / npmDevDependencies ++= Seq(
      "ts-loader" -> "8.0.0",
      "typescript" -> "4.9.5",
      "@types/react" -> "17.0.0",
      "@types/react-dom" -> "17.0.0",
      "@types/node" -> "16.7.13",
    ),
    Compile / npmDependencies ++= Seq(
      "react" -> reactV,
      "react-dom" -> reactV,
      "es6-shim" -> "0.35.7",
      "react-plotly.js" -> reactPlotlyV,
      "plotly.js" -> plotlyV,
      "@stoplight/elements" -> stoplightElementsV,
      "mkdirp" -> "1.0.0",
    ),
    webpackNodeArgs := nodeLegacySslIfAvailable,
    // Scalajs-bundler 0.21.1 updates to webpack 5 but doesn't inform webpack that the scalajs-based file it emits is
    // an entrypoint -- therefore webpack emits an error saying effectively, "no entrypoint" that we must ignore.
    // This aggressively ignores all warnings from webpack, which is more than necessary, but trivially works
    webpackExtraArgs := Seq("--ignore-warnings-message", "/.*/"),
    fastOptJS / webpackConfigFile := Some(baseDirectory.value / "dev.webpack.config.js"),
    fastOptJS / webpackDevServerExtraArgs := Seq("--inline", "--hot"),
    fullOptJS / webpackConfigFile := Some(baseDirectory.value / "prod.webpack.config.js"),
    Test / webpackConfigFile := Some(baseDirectory.value / "common.webpack.config.js"),
    test := {},
    useYarn := true,
    yarnExtraArgs := Seq("--frozen-lockfile"),
  )

// Streaming graph application built on top of the Quine library
lazy val `quine`: Project = project
  .settings(commonSettings)
  .dependsOn(
    `quine-core` % "compile->compile;test->test",
    `quine-cypher` % "compile->compile;test->test",
    `quine-endpoints`.jvm % "compile->compile;test->test",
    `quine-gremlin`,
    `quine-cassandra-persistor`,
    `quine-mapdb-persistor`,
    `quine-rocksdb-persistor`,
  )
  .settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackV,
      "com.github.davidb" % "metrics-influxdb" % metricsInfluxdbV,
      "com.github.jnr" % "jnr-posix" % jnrPosixV,
      "com.github.pjfanning" %% "pekko-http-circe" % pekkoHttpCirceV,
      "com.github.pureconfig" %% "pureconfig" % pureconfigV,
      "com.github.scopt" %% "scopt" % scoptV,
      "com.google.api.grpc" % "proto-google-common-protos" % protobufCommonV,
      "com.github.ben-manes.caffeine" % "caffeine" % caffeineV,
      "com.github.blemale" %% "scaffeine" % scaffeineV,
      "com.google.protobuf" % "protobuf-java" % protobufV,
      "com.softwaremill.sttp.tapir" %% "tapir-pekko-http-server" % tapirV,
      "com.softwaremill.sttp.tapir" %% "tapir-openapi-docs" % tapirV,
      "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % tapirV,
      "com.softwaremill.sttp.apispec" %% "openapi-circe-yaml" % openApiCirceYamlV exclude ("io.circe", "circe-yaml"),
      "org.apache.pekko" %% "pekko-http-testkit" % pekkoHttpV % Test,
      "io.circe" %% "circe-yaml" % circeYamlV,
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingV,
      "ch.qos.logback" % "logback-classic" % logbackV,
      "com.softwaremill.sttp.tapir" %% "tapir-sttp-stub-server" % tapirV % Test,
      "org.scalatest" %% "scalatest" % scalaTestV % Test,
      "com.softwaremill.sttp.client3" %% "circe" % "3.10.2" % Test,
      "software.amazon.glue" % "schema-registry-serde" % amazonGlueV, // for its protobuf DynamicSchema utility
      //"commons-io" % "commons-io" % commonsIoV  % Test,
      "io.circe" %% "circe-config" % "0.10.1",
      "io.circe" %% "circe-generic-extras" % "0.14.4",
      "io.circe" %% "circe-yaml-v12" % "0.16.0",
      "io.circe" %% "circe-core" % circeV,
      "io.dropwizard.metrics" % "metrics-core" % dropwizardMetricsV,
      "io.dropwizard.metrics" % "metrics-jmx" % dropwizardMetricsV,
      "io.dropwizard.metrics" % "metrics-jvm" % dropwizardMetricsV,
      "org.apache.pulsar" % "pulsar-client-kafka" % kafkaClientsV,
      "org.apache.pekko" %% "pekko-connectors-csv" % pekkoConnectorsV,
      "org.apache.pekko" %% "pekko-connectors-kafka" % pekkoKafkaV,
      "org.apache.pekko" %% "pekko-connectors-kinesis" % pekkoConnectorsV exclude ("org.rocksdb", "rocksdbjni"),
      // 3 Next deps: override outdated pekko-connectors-kinesis dependencies
      "software.amazon.kinesis" % "amazon-kinesis-client" % amazonKinesisClientV,
      "org.apache.commons" % "commons-csv" % apacheCommonsCsvV,
      "org.apache.commons" % "commons-compress" % apacheCommonsCompressV,
      "com.github.erosb" % "everit-json-schema" % "1.14.4",
      "org.apache.pekko" %% "pekko-connectors-s3" % pekkoConnectorsV,
      "org.apache.pekko" %% "pekko-connectors-sns" % pekkoConnectorsV,
      "org.apache.pekko" %% "pekko-connectors-sqs" % pekkoConnectorsV,
      "org.apache.pekko" %% "pekko-connectors-sse" % pekkoConnectorsV,
      "org.apache.pekko" %% "pekko-connectors-text" % pekkoConnectorsV,
      // pekko-http-xml is not a direct dep, but an older version is pulled in transitively by
      // pekko-connectors-s3 above. All pekko-http module version numbers need to match exactly, or else it throws
      // at startup: "java.lang.IllegalStateException: Detected possible incompatible versions on the classpath."
      "org.apache.pekko" %% "pekko-http-xml" % pekkoHttpV,
      "org.apache.pekko" %% "pekko-stream-testkit" % pekkoV % Test,
      "org.endpoints4s" %% "pekko-http-server" % endpoints4sHttpServerV,
      "org.scalatest" %% "scalatest" % scalaTestV % Test,
      "org.scalatestplus" %% "scalacheck-1-17" % scalaTestScalaCheckV % Test,
      // WebJars (javascript dependencies masquerading as JARs)
      "org.webjars" % "bootstrap" % bootstrapV,
      "org.webjars" % "ionicons" % ioniconsV,
      "org.webjars" % "jquery" % jqueryV,
      "org.webjars" % "webjars-locator" % webjarsLocatorV,
      "org.webjars.bowergithub.plotly" % "plotly.js" % plotlyV,
      "org.webjars.npm" % "sugar-date" % sugarV,
      "org.webjars.npm" % "vis-network" % visNetworkV,
      "org.apache.avro" % "avro" % avroV,
      // transitive dependency of rsocket-transport-netty. Current version required by rsocket-transport-netty has vulnerabilities
      "io.projectreactor.netty" % "reactor-netty-core" % "1.0.48",
      // transitive dependency of rsocket-transport-netty. Current version required by rsocket-transport-netty has vulnerabilities
      "io.projectreactor.netty" % "reactor-netty-http" % "1.0.48",
      "io.rsocket" % "rsocket-core" % rsocketV,
      "io.rsocket" % "rsocket-transport-netty" % rsocketV,
    ),
  )
  .enablePlugins(WebScalaJSBundlerPlugin)
  .settings(
    scalaJSProjects := Seq(`quine-browser`),
    Assets / pipelineStages := Seq(scalaJSPipeline),
  )
  .enablePlugins(BuildInfoPlugin, Packaging, Docker, Ecr)
  .settings(
    startupMessage := "",
    buildInfoKeys := Seq[BuildInfoKey](version, startupMessage),
    buildInfoPackage := "com.thatdot.quine.app",
  )

lazy val `quine-docs`: Project = {
  val docJson = Def.setting((Compile / sourceManaged).value / "reference" / "openapi.json")
  val cypherTable1 = Def.setting((Compile / sourceManaged).value / "reference" / "cypher-builtin-functions.md")
  val cypherTable2 =
    Def.setting((Compile / sourceManaged).value / "reference" / "cypher-user-defined-functions.md")
  val cypherTable3 =
    Def.setting((Compile / sourceManaged).value / "reference" / "cypher-user-defined-procedures.md")

  val generateDocs = TaskKey[Unit]("generateDocs", "Generate documentation tables for the Quine (Mkdocs) project")

  Project("quine-docs", file("quine-docs"))
    .dependsOn(`quine`)
    .settings(commonSettings)
    .settings(
      generateDocs := Def
        .sequential(
          Def.taskDyn {
            (Compile / runMain)
              .toTask(
                List(
                  " com.thatdot.quine.docs.GenerateCypherTables",
                  cypherTable1.value.getAbsolutePath,
                  cypherTable2.value.getAbsolutePath,
                  cypherTable3.value.getAbsolutePath,
                ).mkString(" "),
              )
          },
          Def.taskDyn {
            (Compile / runMain)
              .toTask(s" com.thatdot.quine.docs.GenerateOpenApi ${docJson.value.getAbsolutePath}")
          },
        )
        .value,
    )
    .settings(
      libraryDependencies ++= Seq(
        "org.pegdown" % "pegdown" % pegdownV,
        "org.parboiled" % "parboiled-java" % parboiledV,
        "org.scalatest" %% "scalatest" % scalaTestV % Test,
      ),
    )
}

// Spurious warnings
Global / excludeLintKeys += `quine-browser` / webpackNodeArgs
Global / excludeLintKeys += `quine-browser` / webpackExtraArgs
