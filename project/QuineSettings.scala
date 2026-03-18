import sbt._
import sbt.Keys._
import org.portablescala.sbtplatformdeps.PlatformDepsPlugin.autoImport._
import scalajsbundler.sbtplugin.ScalaJSBundlerPlugin.autoImport._

import scala.collection.compat.toOptionCompanionExtension
import scala.sys.process._
import scala.util.Try

object QuineSettings {

  val scalaV = "2.13.18"

  val nodeLegacySslArg = "--openssl-legacy-provider"
  // See if node accepts this arg. Give it an expression to evaluate {} so it returns instead of entering the repl
  def nodeLegacySslIfAvailable: Seq[String] =
    if (Try(Seq("node", nodeLegacySslArg, "-e", "{}") ! ProcessLogger(_ => ())).toOption.contains(0))
      Seq(nodeLegacySslArg)
    else Seq()

  val integrationTestTag = "com.thatdot.quine.test.tags.IntegrationTest"
  val licenseRequiredTestTag = "com.thatdot.quine.test.tags.LicenseRequiredTest"

  lazy val Integration = config("integration").extend(Test)
  lazy val LicenseTest = config("licenseTest").extend(Test)

  val commonSettings: Seq[Setting[_]] = Seq(
    organization := "com.thatdot",
    organizationName := "thatDot Inc.",
    organizationHomepage := Some(url("https://www.thatdot.com")),
    autoAPIMappings := true,
    scalacOptions ++= Seq(
      "-language:postfixOps",
      "-encoding",
      "utf8",
      "-feature",
      "-unchecked",
      "-deprecation",
      "-release",
      "11",
      "-Xlint:_,-byname-implicit",
      "-Wdead-code",
      "-Wnumeric-widen",
      "-Wvalue-discard",
      "-Wunused:imports",
      "-Wunused:privates,locals,patvars",
    ) ++ Option.when(insideCI.value)("-Werror"),
    javacOptions ++= Seq("--release", "11"),
    // Circe is binary compatible between 0.13 and 0.14
    // Circe projects from other orgs sometimes pull in older versions of circe (0.13):
    // As of Mar 8 2023, ujson-circe
    // This prevents sbt from erroring with:
    // "found version conflict(s) in library dependencies; some are suspected to be binary incompatible"
    libraryDependencySchemes ++= Seq(
      "io.circe" %% "circe-core" % VersionScheme.Always,
      "io.circe" %% "circe-parser" % VersionScheme.Always,
    ),
    Test / testOptions ++= Seq(
      //Include a report at the end of a test run with details on any failed tests:
      //  use oG for full stack traces, oT for short ones
      Tests.Argument(TestFrameworks.ScalaTest, "-oT"),
      Tests.Argument(TestFrameworks.ScalaTest, "-l", integrationTestTag),
      Tests.Argument(TestFrameworks.ScalaTest, "-l", licenseRequiredTestTag),
    ),
    excludeDependencies ++= Seq(
      ExclusionRule("commons-logging", "commons-logging"),
      // Exclude old lz4-java; we use at.yawk.lz4:lz4-java instead (CVE-2025-66566, CVE-2025-12183)
      ExclusionRule("org.lz4", "lz4-java"),
    ),
    libraryDependencies ++= Seq(
      "org.slf4j" % "jcl-over-slf4j" % "2.0.17",
    ),
  )

  /* Settings for projects with integrationTests */
  val integrationSettings: Seq[Setting[_]] = Seq(
    Integration / testOptions -= Tests.Argument(TestFrameworks.ScalaTest, "-l", integrationTestTag),
    Integration / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-n", integrationTestTag),
    Integration / parallelExecution := false,
  ) ++ inConfig(Integration)(Defaults.testTasks)

  /* Settings for projects with license-required tests */
  val licenseTestSettings: Seq[Setting[_]] = Seq(
    LicenseTest / testOptions -= Tests.Argument(TestFrameworks.ScalaTest, "-l", licenseRequiredTestTag),
    LicenseTest / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-n", licenseRequiredTestTag),
    LicenseTest / parallelExecution := false,
    LicenseTest / fork := true,
  ) ++ inConfig(LicenseTest)(Defaults.testTasks)

  val startupMessage = settingKey[String]("If non-empty, print this message on startup")
    .withRank(KeyRanks.Invisible)

  /* Settings for projects using vis-network (CSP-compliant peer build)
   *
   * The peer build avoids dynamic code evaluation (eval), allowing stricter
   * Content Security Policy without 'unsafe-eval' in script-src.
   */
  val visNetworkSettings: Seq[Setting[_]] = Seq(
    Compile / npmDependencies ++= Seq(
      "vis-network" -> Dependencies.visNetworkV,
      "vis-data" -> Dependencies.visDataV,
      "vis-util" -> Dependencies.visUtilV,
      "@egjs/hammerjs" -> Dependencies.egjsHammerjsV,
      "component-emitter" -> Dependencies.componentEmitterV,
      "keycharm" -> Dependencies.keycharmV,
      "uuid" -> Dependencies.uuidV,
    ),
  )
}
