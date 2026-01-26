import sbtassembly.{Assembly, AssemblyPlugin, CustomMergeStrategy, MergeStrategy, PathList}
import sbtassembly.AssemblyKeys.{assembly, assemblyMergeStrategy}
import sbt._
import sbt.Keys.packageOptions

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
  val customMergeStrategy: String => MergeStrategy = {
    case x if Assembly.isConfigFile(x) => appendProjectsLast
    case "version.conf" => MergeStrategy.concat
    case PathList("META-INF", "LICENSES.txt") | "AUTHORS" => MergeStrategy.concat
    case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.discard
    // Discard Kotlin Native metadata files that cause deduplication conflicts.
    // These "nativeMain/default/manifest" and similar files from okio and wire
    // libraries are only relevant for Kotlin Native targets, not JVM.
    case PathList("commonMain", "default", "manifest") => MergeStrategy.discard
    case PathList("nativeMain", "default", "manifest") => MergeStrategy.discard
    case PathList("commonMain", "default", "linkdata", "module") => MergeStrategy.discard
    case PathList("nativeMain", "default", "linkdata", "module") => MergeStrategy.discard
    case PathList("META-INF", "kotlin-project-structure-metadata.json") => MergeStrategy.discard
    case PathList("META-INF", "kotlinx-serialization-core.kotlin_module") => MergeStrategy.first
    case PathList("META-INF", "okio-fakefilesystem.kotlin_module") => MergeStrategy.first
    case PathList("META-INF", "okio.kotlin_module") => MergeStrategy.first
    case PathList("META-INF", "wire-runtime.kotlin_module") => MergeStrategy.first
    case PathList("META-INF", "wire-schema.kotlin_module") => MergeStrategy.first
    case PathList("META-INF", "versions", "9", "OSGI-INF", "MANIFEST.MF") => MergeStrategy.first // from bouncycastle
    case PathList("META-INF", "FastDoubleParser-NOTICE") =>
      MergeStrategy.first // from fasterxml jackson core (and its awssdk shadow)
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
      assembly / assemblyMergeStrategy := customMergeStrategy,
      // GraalVM 25+ uses Multi-Release JARs (MRJAR). This manifest attribute must be preserved
      // in the assembled JAR for Truffle/GraalJS to initialize correctly.
      // See: https://www.graalvm.org/latest/reference-manual/embed-languages/#uber-jar-file-creation
      assembly / packageOptions += Package.ManifestAttributes("Multi-Release" -> "true"),
    )
}
