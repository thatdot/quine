import QuineSettings._
import Dependencies._

addCommandAlias("scala212", "++" + scalaV212)
addCommandAlias("scala213", "++" + scalaV213)
addCommandAlias("fixall", "; scalafixAll; scalafmtAll; scalafmtSbt")

// Core streaming graph interpreter
lazy val `quine-core`: Project = project
  .settings(commonSettings)
  .settings(`scala 2.12 to 2.13`)
  .settings(
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % shapelessV,
      "com.google.guava" % "guava" % guavaV,
      "org.scala-lang.modules" %% "scala-java8-compat" % (if (scalaVersion.value == scalaV212) "0.9.1" else "1.0.2"),
      "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionCompatV,
      "com.typesafe.akka" %% "akka-actor" % akkaV,
      "com.typesafe.akka" %% "akka-stream" % akkaV,
      "com.typesafe.akka" %% "akka-slf4j" % akkaV,
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingV,
      "io.dropwizard.metrics" % "metrics-core" % dropwizardMetricsV,
      "com.lihaoyi" %% "ujson" % ujsonV,
      "org.msgpack" % "msgpack-core" % msgPackV,
      "org.apache.commons" % "commons-text" % commonsTextV,
      "commons-codec" % "commons-codec" % commonsCodecV,
      "com.47deg" %% "memeid4s" % memeIdV,
      // Testing
      "org.scalatest" %% "scalatest" % scalaTestV % Test,
      "org.scalacheck" %% "scalacheck" % scalaCheckV % Test,
      "org.scalatestplus" %% "scalacheck-1-15" % scalaTestScalaCheckV % Test,
      "org.gnieh" % "logback-config" % logbackConfigV % Test
    ),
    // Compile different files depending on scala version
    Compile / unmanagedSourceDirectories += {
      val sourceDir = (Compile / sourceDirectory).value
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n >= 13 => sourceDir / "scala-2.13"
        case _ => sourceDir / "scala-2.12"
      }
    }
  )
  .enablePlugins(BuildInfoPlugin, FlatcPlugin)
  .settings(
    buildInfoOptions := Seq(BuildInfoOption.BuildTime),
    buildInfoKeys := Seq[BuildInfoKey](
      version,
      git.gitHeadCommit,
      git.gitUncommittedChanges,
      git.gitHeadCommitDate,
      BuildInfoKey.action("javaVmName")(scala.util.Properties.javaVmName),
      BuildInfoKey.action("javaVendor")(scala.util.Properties.javaVendor),
      BuildInfoKey.action("javaVersion")(scala.util.Properties.javaVersion)
    ),
    buildInfoPackage := "com.thatdot.quine"
  )

// MapDB implementation of a Quine persistor
lazy val `quine-mapdb-persistor`: Project = project
  .settings(commonSettings)
  .settings(`scala 2.12 to 2.13`)
  .dependsOn(`quine-core` % "compile->compile;test->test")
  .settings(
    /* `net.jpountz.lz4:lz4` was moved to `org.lz4:lz4-java`, but MapDB hasn't
     * adapted to this change quickly. However, since other parts of the Java
     * ecosystem _have_ (example: `akka-stream kafka`), we need to exclude the
     * bad JAR and explicitly pull in the good one.
     *
     * This is fixed in <https://github.com/jankotek/mapdb/pull/992>
     */
    libraryDependencies ++= Seq(
      ("org.mapdb" % "mapdb" % mapDbV).exclude("net.jpountz.lz4", "lz4"),
      "org.lz4" % "lz4-java" % lz4JavaV
    )
  )

// RocksDB implementation of a Quine persistor
lazy val `quine-rocksdb-persistor`: Project = project
  .settings(commonSettings)
  .settings(`scala 2.12 to 2.13`)
  .dependsOn(`quine-core` % "compile->compile;test->test")
  .settings(
    libraryDependencies ++= Seq(
      "org.rocksdb" % "rocksdbjni" % rocksdbV
    )
  )

// Cassandra implementation of a Quine persistor
lazy val `quine-cassandra-persistor`: Project = project
  .settings(commonSettings)
  .settings(`scala 2.12 to 2.13`)
  .dependsOn(`quine-core` % "compile->compile;test->test")
  .settings(
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % catsV,
      "com.datastax.oss" % "java-driver-query-builder" % cassandraClientV,
      "com.github.nosan" % "embedded-cassandra" % embeddedCassandraV % Test
    )
  )

// Parser and interepreter for a subset of [Gremlin](https://tinkerpop.apache.org/gremlin.html)
lazy val `quine-gremlin`: Project = project
  .settings(commonSettings)
  .settings(`scala 2.12 to 2.13`)
  .dependsOn(`quine-core`, `quine-mapdb-persistor` % "test->test")
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-parser-combinators" % scalaParserCombinatorsV,
      "org.apache.commons" % "commons-text" % commonsTextV,
      "org.scalatest" %% "scalatest" % scalaTestV % Test,
      "org.gnieh" % "logback-config" % logbackConfigV % Test
    )
  )

// Compiler for compiling [Cypher](https://neo4j.com/docs/cypher-manual/current/) into Quine queries
lazy val `quine-cypher`: Project = project
  .settings(commonSettings)
  .settings(`scala 2.12`)
  .dependsOn(`quine-core` % "compile->compile;test->test")
  .settings(
    scalacOptions += "-language:reflectiveCalls",
    libraryDependencies ++= Seq(
      "org.opencypher" % "expressions-9.0" % openCypherV,
      "org.opencypher" % "front-end-9.0" % openCypherV,
      "org.opencypher" % "parser-9.0" % openCypherV,
      "org.opencypher" % "util-9.0" % openCypherV,
      "org.typelevel" %% "cats-core" % catsV,
      "io.github.classgraph" % "classgraph" % "4.8.147",
      "org.scalatest" %% "scalatest" % scalaTestV % Test,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaV % Test
    ),
    addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
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
  .settings(`scala 2.12 to 2.13`)
  .enablePlugins(ScalaJSPlugin)
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-js" %%% "scalajs-dom" % scalajsDomV
    )
  )

// REST API specifications for `quine`-based applications
lazy val `quine-endpoints` = crossProject(JSPlatform, JVMPlatform)
  .crossType(CrossType.Pure)
  .in(file("quine-endpoints"))
  .settings(commonSettings)
  .settings(`scala 2.12 to 2.13`)
  .settings(
    libraryDependencies ++= Seq(
      "org.endpoints4s" %%% "algebra" % endpoints4sDefaultV,
      "org.endpoints4s" %%% "json-schema-generic" % endpoints4sDefaultV,
      "org.endpoints4s" %%% "openapi" % endpoints4sOpenapiV,
      "org.scalacheck" %%% "scalacheck" % scalaCheckV % Test
    )
  )
  .jsSettings(
    // Provides an implementatAllows us to use java.time.Instant in Scala.js
    libraryDependencies += "io.github.cquiroz" %%% "scala-java-time" % scalaJavaTimeV
  )

// Quine web application
lazy val `quine-browser`: Project = project
  .settings(commonSettings, slinkySettings)
  .settings(`scala 2.12 to 2.13`)
  .dependsOn(`quine-endpoints`.js, `visnetwork-facade`)
  .enablePlugins(ScalaJSBundlerPlugin)
  .settings(
    libraryDependencies ++= Seq(
      "org.scala-js" %%% "scalajs-dom" % scalajsDomV,
      "org.scala-js" %%% "scala-js-macrotask-executor" % scalajsMacroTaskExecutorV,
      "org.endpoints4s" %%% "xhr-client" % endpoints4sXhrClientV
    ),
    Compile / npmDependencies ++= Seq(
      "react-plotly.js" -> reactPlotlyV,
      "@stoplight/elements" -> stoplightElementsV
    ),
    fastOptJS / webpackConfigFile := Some(baseDirectory.value / "dev.webpack.config.js"),
    fullOptJS / webpackConfigFile := Some(baseDirectory.value / "prod.webpack.config.js"),
    Test / webpackConfigFile := Some(baseDirectory.value / "common.webpack.config.js"),
    test := {}
  )

// Streaming graph application built on top of the Quine library
lazy val `quine`: Project = project
  .settings(commonSettings)
  .settings(`scala 2.12`)
  .dependsOn(
    `quine-core` % "compile->compile;test->test",
    `quine-cypher`,
    `quine-endpoints`.jvm % "compile->compile;test->test",
    `quine-gremlin`,
    `quine-cassandra-persistor`,
    `quine-mapdb-persistor`,
    `quine-rocksdb-persistor`
  )
  .settings(
    version := quineAppV,
    libraryDependencies ++= Seq(
      "org.gnieh" % "logback-config" % logbackConfigV,
      "ch.qos.logback" % "logback-classic" % logbackV,
      "com.github.pureconfig" %% "pureconfig" % pureconfigV,
      "io.dropwizard.metrics" % "metrics-core" % dropwizardMetricsV,
      "io.dropwizard.metrics" % "metrics-jmx" % dropwizardMetricsV,
      "io.dropwizard.metrics" % "metrics-jvm" % dropwizardMetricsV,
      "com.github.davidb" % "metrics-influxdb" % metricsInfluxdbV,
      "software.amazon.awssdk" % "netty-nio-client" % nettyNioClientV,
      "com.typesafe.akka" %% "akka-stream-contrib" % akkaStreamContribV,
      "com.typesafe.akka" %% "akka-stream-kafka" % alpakkaKafkaV,
      "com.lightbend.akka" %% "akka-stream-alpakka-kinesis" % alpakkaKinesisV exclude ("org.rocksdb", "rocksdbjni"),
      "com.lightbend.akka" %% "akka-stream-alpakka-sqs" % alpakkaSQSV,
      "com.lightbend.akka" %% "akka-stream-alpakka-sse" % alpakkaSseV,
      "com.lightbend.akka" %% "akka-stream-alpakka-sns" % alpakkaSnsV,
      "com.lightbend.akka" %% "akka-stream-alpakka-csv" % alpakkaCsvV,
      "com.lightbend.akka" %% "akka-stream-alpakka-text" % alpakkaTextV,
      // akka-http-xml is not a direct dep, but pulled in transitively by Alpakka modules above.
      // All akka-http module version numbers need to match exactly, or else it
      // throws at startup: "java.lang.IllegalStateException: Detected possible incompatible versions on the classpath."
      "com.typesafe.akka" %% "akka-http-xml" % akkaHttpV,
      "org.scalatest" %% "scalatest" % scalaTestV % Test,
      "org.scalatestplus" %% "scalacheck-1-15" % scalaTestScalaCheckV % Test,
      "org.endpoints4s" %% "algebra-json-schema" % endpoints4sDefaultV,
      "org.endpoints4s" %% "json-schema-generic" % endpoints4sDefaultV,
      "org.endpoints4s" %% "akka-http-server" % endpoints4sHttpServerV,
      // WebJars (javascript dependencies masquerading as JARs)
      "org.webjars" % "webjars-locator" % webjarsLocatorV,
      "org.webjars.npm" % "vis-network" % visNetworkV,
      "org.webjars" % "ionicons" % ioniconsV,
      "org.webjars" % "jquery" % jqueryV,
      "org.webjars" % "bootstrap" % bootstrapV,
      "org.webjars.npm" % "sugar-date" % sugarV,
      "org.webjars.bowergithub.plotly" % "plotly.js" % plotlyV,
      "com.google.guava" % "guava" % guavaV,
      "com.google.protobuf" % "protobuf-java" % protobufV,
      "com.github.jnr" % "jnr-posix" % jnrPosixV,
      "com.github.scopt" %% "scopt" % scoptV,
      "org.yaml" % "snakeyaml" % snakeYamlV
    )
  )
  .enablePlugins(WebScalaJSBundlerPlugin)
  .settings(
    scalaJSProjects := Seq(`quine-browser`),
    Assets / pipelineStages := Seq(scalaJSPipeline)
  )
  .enablePlugins(BuildInfoPlugin, Packaging)
  .settings(
    startupMessage := "",
    buildInfoKeys := Seq[BuildInfoKey](version, startupMessage),
    buildInfoPackage := "com.thatdot.quine.app"
  )

lazy val `quine-docs`: Project = {
  val docJson = Def.task((Compile / paradox / sourceManaged).value / "reference" / "openapi.json")
  val cypherTable1 = Def.task((Compile / paradox / sourceManaged).value / "reference" / "cypher-builtin-functions.md")
  val cypherTable2 =
    Def.task((Compile / paradox / sourceManaged).value / "reference" / "cypher-user-defined-functions.md")
  val cypherTable3 =
    Def.task((Compile / paradox / sourceManaged).value / "reference" / "cypher-user-defined-procedures.md")
  val recipesFolder =
    Def.task((Compile / paradox / sourceManaged).value / "recipes")
  Project("quine-docs", file("quine-docs"))
    .dependsOn(`quine`)
    .settings(commonSettings)
    .settings(`scala 2.12`)
    .enablePlugins(ParadoxThatdot, GhpagesPlugin)
    .settings(
      version := quineAppV,
      projectName := "Quine",
      git.remoteRepo := "git@github.com:thatdot/quine.io.git",
      ghpagesBranch := "main",
      ghpagesCleanSite / excludeFilter := { (f: File) =>
        (ghpagesRepository.value / "CNAME").getCanonicalPath == f.getCanonicalPath
      },
      // Same as `paradox` itself
      libraryDependencies ++= Seq(
        "org.pegdown" % "pegdown" % pegdownV,
        "org.parboiled" % "parboiled-java" % parboiledV
      ),
      Compile / paradoxProperties ++= Map(
        "snip.github_link" -> "false",
        "snip.quine.base_dir" -> (`quine` / baseDirectory).value.getAbsolutePath,
        "material.repo" -> "https://github.com/thatdot/quine",
        "material.repo.type" -> "github",
        "material.social" -> "https://that.re/quine-slack",
        "material.social.type" -> "slack",
        "include.generated.base_dir" -> (Compile / paradox / sourceManaged).value.toString,
        "project.name" -> projectName.value,
        "logo.link.title" -> "Quine",
        "quine.jar" -> s"quine-${version.value}.jar"
      ),
      description := "Quine is a streaming graph interpreter meant to trigger actions in real-time based on complex patterns pulled from high-volume streaming data",
      Compile / paradoxMarkdownToHtml / sourceGenerators += Def.taskDyn {
        (Compile / runMain)
          .toTask(s" com.thatdot.quine.docs.GenerateOpenApi ${docJson.value.getAbsolutePath}")
          .map(_ => Seq()) // return no files because files returned are supposed to be markdown
      },
      // Register the `openapi.json` file here
      Compile / paradox / mappings ++= List(
        docJson.value -> "reference/openapi.json"
      ),
      // ---
      // Uncomment to build the recipe template pages
      // then add * @ref:[Recipes](recipes/index.md) into docs.md
      // ---
      //Compile / paradoxMarkdownToHtml / sourceGenerators += Def.taskDyn {
      //  val inDir: File = (quine / baseDirectory).value / "recipes"
      //  val outDir: File = (Compile / paradox / sourceManaged).value / "recipes"
      //  (Compile / runMain)
      //    .toTask(s" com.thatdot.quine.docs.GenerateRecipeDirectory ${inDir.getAbsolutePath} ${outDir.getAbsolutePath}")
      //    .map(_ => (outDir * "*.md").get)
      //},
      Compile / paradoxNavigationDepth := 3,
      Compile / paradoxNavigationExpandDepth := Some(3),
      paradoxRoots := List("index.html", "docs.html", "about.html", "download.html"),
      Compile / paradoxMarkdownToHtml / sourceGenerators += Def.taskDyn {
        (Compile / runMain)
          .toTask(
            List(
              " com.thatdot.quine.docs.GenerateCypherTables",
              cypherTable1.value.getAbsolutePath,
              cypherTable2.value.getAbsolutePath,
              cypherTable3.value.getAbsolutePath
            ).mkString(" ")
          )
          .map(_ => Nil) // files returned are included, not top-level
      },
      Compile / paradoxMaterialTheme ~= {
        _.withCustomStylesheet("assets/quine.css")
          .withLogo("assets/images/quine_logo.svg")
          .withColor("white", "quine-blue")
          .withFavicon("assets/images/favicon.svg")
      },
      Compile / overlayDirectory := (`paradox-overlay` / baseDirectory).value
    )
}

lazy val `paradox-overlay`: Project = project

// Spurious warnings
Global / excludeLintKeys += `quine-docs` / Paradox / paradoxNavigationExpandDepth
Global / excludeLintKeys += `quine-docs` / Paradox / paradoxNavigationDepth
