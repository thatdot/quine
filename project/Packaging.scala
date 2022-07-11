import sbtassembly.{Assembly, AssemblyPlugin, MergeStrategy, PathList}
import sbtassembly.AssemblyKeys.{assembly, assemblyMergeStrategy}
import sbt._

/* Plugin for building a fat JAR */
object Packaging extends AutoPlugin {

  override def requires = AssemblyPlugin
  override def trigger = noTrigger

  // Assembly merge strategy
  private val reverseConcat: MergeStrategy = new MergeStrategy {
    val name = "reverseConcat"
    def apply(tempDir: File, path: String, files: Seq[File]): Either[String, Seq[(File, String)]] =
      MergeStrategy.concat(tempDir, path, files.reverse)
  }

  /* This decides how to aggregate files from different JARs into one JAR.
   *
   *   - resolves conflicts between duplicate files in different JARs
   *   - allows for removing entirely unnecessary resources from output JAR
   */
  private val mergeStrategy: String => MergeStrategy = {
    case x if Assembly.isConfigFile(x) => reverseConcat
    case "version.conf" => reverseConcat
    case PathList("META-INF", "LICENSES.txt") | "AUTHORS" => MergeStrategy.concat
    case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.discard
    case PathList("codegen-resources", _) => MergeStrategy.discard
    case PathList(ps @ _*) if ps.last == "module-info.class" => MergeStrategy.discard
    case "JS_DEPENDENCIES" => MergeStrategy.discard
    // See https://github.com/akka/akka/issues/29456
    case PathList("google", "protobuf", file) if file.split('.').last == "proto" => MergeStrategy.first
    case PathList("google", "protobuf", "compiler", "plugin.proto") => MergeStrategy.first
    case other => MergeStrategy.defaultMergeStrategy(other)
  }

  override lazy val projectSettings =
    Seq(
      assembly / assemblyMergeStrategy := mergeStrategy
    )
}
