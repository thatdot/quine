import sbt._
import sbt.Keys._
import org.portablescala.sbtplatformdeps.PlatformDepsPlugin.autoImport._
import scalajsbundler.sbtplugin.ScalaJSBundlerPlugin.autoImport._
import scala.sys.process._

object QuineSettings {

  val scalaV212 = "2.12.17"
  val scalaV213 = "2.13.10"
  val `scala 2.12 to 2.13`: Seq[Setting[_]] = Seq(
    scalaVersion := scalaV212,
    crossScalaVersions := Seq(scalaV212, scalaV213)
  )
  val `scala 2.12`: Seq[Setting[_]] = Seq(
    scalaVersion := scalaV212,
    crossScalaVersions := Seq(scalaV212)
  )

  val nodeLegacySslArg = "--openssl-legacy-provider"
  // See if node acceps this arg. Give it an expression to evaluate {} so it returns instead of entering the repl
  def nodeLegacySslIfAvailable: Seq[String] =
    if (Seq("node", nodeLegacySslArg, "-e", "{}").! == 0) Seq(nodeLegacySslArg) else Seq()

  val commonSettings: Seq[Setting[_]] = Seq(
    organization := "com.thatdot",
    organizationName := "thatDot Inc.",
    organizationHomepage := Some(url("http://thatdot.com")),
    scalacOptions ++= Seq(
      "-language:implicitConversions",
      "-language:postfixOps",
      "-encoding",
      "utf8",
      "-feature",
      "-unchecked",
      "-deprecation",
      "-release",
      "11"
    ),
    autoAPIMappings := true,
    scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) =>
        Seq(
          "-Xlint:-unused,-missing-interpolator,_",
          "-Ywarn-dead-code",
          "-Ywarn-numeric-widen",
          "-Ywarn-value-discard",
          "-Ywarn-unused:privates,locals,patvars,imports",
          "-Ypartial-unification"
        ) ++ (if (insideCI.value) Seq("-Xfatal-warnings") else Seq.empty)
      case Some((2, 13)) =>
        Seq(
          "-Xlint:-byname-implicit,-unused,-missing-interpolator,_",
          "-Wdead-code",
          "-Wnumeric-widen",
          "-Wvalue-discard",
          // "-Wunused:imports", // See https://github.com/scala/scala-collection-compat/issues/240
          "-Wunused:privates,locals,patvars"
        ) ++ (if (insideCI.value) Seq("-Werror") else Seq.empty)
      case _ =>
        Seq.empty
    }),
    javacOptions ++= Seq("--release", "11")
  )

  /* Settings for building a Scala.js/React webapp using Slinky
   *
   * See the docs at <https://slinky.dev/docs/installation/>
   */
  val slinkySettings: Seq[Setting[_]] = Seq(
    libraryDependencies ++= Seq(
      "me.shadaj" %%% "slinky-core" % Dependencies.slinkyV,
      "me.shadaj" %%% "slinky-web" % Dependencies.slinkyV
    ),
    /* This is disabled by default because it slows down the build. However, it
     * can be very useful to re-enable when debugging a JS error (stack traces
     * will have informative errors, it will be possible to set breakpoints in
     * the Scala code viewed from the browser)
     */
    webpackEmitSourceMaps := false,
    Test / requireJsDomEnv := true,
    Compile / npmDevDependencies ++= Seq(
      "css-loader" -> "3.5.3",
      "style-loader" -> "1.2.1",
      "url-loader" -> "4.1.0",
      "webpack-merge" -> "4.1.2"
    ),
    Compile / npmDependencies ++= Seq(
      "react" -> Dependencies.reactV,
      "react-dom" -> Dependencies.reactV
    ),
    // Needed for the `@react` macro annotation in `slinky`
    libraryDependencies ++= (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) =>
        Seq(compilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full))
      case _ =>
        Nil
    }),
    scalacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) =>
        Nil
      case _ =>
        Seq("-Ymacro-annotations")
    }),
    doc := file("phony-no-doc-file"), // Avoid <https://github.com/shadaj/slinky/issues/380>
    packageDoc / publishArtifact := false // Avoid <https://github.com/shadaj/slinky/issues/380>
  )

  val startupMessage = settingKey[String]("If non-empty, print this message on startup")
    .withRank(KeyRanks.Invisible)
}
