import sbtassembly.{Assembly, AssemblyPlugin, CustomMergeStrategy, MergeStrategy, PathList}
import sbtassembly.AssemblyKeys.{assembly, assemblyMergeStrategy}
import sbt._

/* Plugin for building a fat JAR */
object Packaging extends AutoPlugin {

  override def requires = AssemblyPlugin

  // Assembly merge strategy
  private val appendProjectsLast: MergeStrategy = CustomMergeStrategy("appendProjectsLast") { conflicts =>
    val (projects, libraries) = conflicts.partition(_.isProjectDependency)
    // Make sure our reference.confs are appended _after_ reference.confs in libraries
    MergeStrategy.concat(libraries ++ projects)
  }

  /* This decides how to aggregate files from different JARs into one JAR.
   *
   *   - resolves conflicts between duplicate files in different JARs
   *   - allows for removing entirely unnecessary resources from output JAR
   */
  private val mergeStrategy: String => MergeStrategy = {
    case x if Assembly.isConfigFile(x) => appendProjectsLast
    case "version.conf" => MergeStrategy.concat
    case PathList("META-INF", "LICENSES.txt") | "AUTHORS" => MergeStrategy.concat
    case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.discard
    case PathList("META-INF", "native-image", "org.mongodb", "bson", "native-image.properties") => MergeStrategy.discard
    case PathList("codegen-resources", _) => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last == "module-info.class" => MergeStrategy.discard
    case PathList("META-INF", "native-image", "io.netty", "netty-common", "native-image.properties") =>
      MergeStrategy.discard
    case PathList("META-INF", "native-image", "io.netty", "codec-http", "native-image.properties") =>
      MergeStrategy.discard
    case "findbugsExclude.xml" => MergeStrategy.discard
    case "JS_DEPENDENCIES" => MergeStrategy.discard
    // See https://github.com/akka/akka/issues/29456
    case PathList("google", "protobuf", file) if file.split('.').last == "proto" => MergeStrategy.first
    case PathList("google", "protobuf", "compiler", "plugin.proto") => MergeStrategy.first
    case PathList("org", "apache", "avro", "reflect", _) => MergeStrategy.first
    case other => MergeStrategy.defaultMergeStrategy(other)
  }

  override lazy val projectSettings =
    Seq(
      assembly / assemblyMergeStrategy := mergeStrategy
    )
}
