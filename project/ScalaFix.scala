import sbt._
import sbt.Keys.{semanticdbEnabled, semanticdbVersion}
import scalafix.sbt.ScalafixPlugin

// Extra scalafix configuration and dependencies
object ScalaFix extends AutoPlugin {

  override def requires = ScalafixPlugin
  override def trigger = allRequirements

  import ScalafixPlugin.autoImport._

  override lazy val projectSettings = Seq(
    semanticdbEnabled := true, // enable SemanticDB
    semanticdbVersion := scalafixSemanticdb.revision, // use Scalafix compatible version
    ThisBuild / scalafixDependencies ++= Seq(
      "org.scala-lang" %% "scala-rewrites" % "0.1.3",
      "com.github.liancheng" %% "organize-imports" % "0.6.0"
    )
  )
}
