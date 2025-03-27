import sbt._
import sbt.Keys._
import sbt.util.CacheImplicits._

import scala.util.Properties

object FlatcPlugin extends AutoPlugin {
  import Dependencies.flatbuffersV

  object autoImport {
    val flatcOptions = SettingKey[Seq[String]]("flatc-options", "Additional options to be passed to flatc")

    val flatcSources = SettingKey[Seq[File]]("flatc-sources", "Directories to look for source files")

    val flatcOutput = SettingKey[File]("flatc-output", "Directory into which outputs will be written")

    val flatcDependency = SettingKey[Option[URL]]("flatc-dependency", "URL for zipped binary artifact for flatc")

    val flatcExecutable = TaskKey[File](
      "flatc-executable",
      "Path to a flatc executable. Default downloads flatcDependency from Github.",
    )
  }

  import autoImport._

  // Use `buildSettings` to download the `flatc` executable only once (not once per project)
  override def buildSettings: Seq[Def.Setting[_]] =
    Seq(
      flatcDependency := {
        val prefix = s"https://github.com/google/flatbuffers/releases/download/v$flatbuffersV/"
        val suffixOpt =
          if (Properties.isMac) Some("Mac.flatc.binary.zip")
          else if (Properties.isWin) Some("Windows.flatc.binary.zip")
          else if (Properties.isLinux) Some("Linux.flatc.binary.clang++-18.zip")
          else None

        suffixOpt.map(suffix => url(prefix + suffix))
      },
      // This must match the version of the jar we download from Maven
      flatcExecutable := {
        val outputDirectory = (ThisBuild / baseDirectory).value / BuildPaths.DefaultTargetName / "flatc"
        val url: URL = flatcDependency.value.getOrElse {
          val os = Properties.osName
          val suggestion = "set flatcExecutable := file(path-to-flatc)"
          throw new sbt.internal.util.MessageOnlyException(
            s"Could not identify flatc binary for $os (try manually setting `$suggestion`)",
          )

        }
        val flatcStore = streams.value.cacheStoreFactory.make("flatcStore")

        /* Fetch the right `flatc` binary
         *
         * @param file directory into which to place the `flatc` binary
         * @param url URL from which to download a ZIP of the `flatc` binary
         * @return path to the downloaded flatc
         */
        val getFlatc: ((File, URL)) => File = Cache.cached[(File, URL), File](flatcStore) {
          case (outputDirectory, url) =>
            val logger = streams.value.log
            logger.info(s"Downloading flatc from $url...")
            val files = IO.unzipURL(url, outputDirectory)
            assert(files.size == 1, "Only expected a single file in the zip file when downloading flatc")
            val flatcPath = files.head
            if (IO.isPosix) IO.chmod("rwxr--r--", flatcPath)
            logger.info(s"Saved flatc to $flatcPath")
            flatcPath
        }

        getFlatc(outputDirectory, url)

      },
    )

  override def projectSettings: Seq[Def.Setting[_]] =
    Seq(
      flatcOptions := Seq("--java"),
      flatcSources := Seq((Compile / sourceDirectory).value / "fbs"),
      flatcOutput := (Compile / sourceManaged).value / "fbs",
      Compile / sourceGenerators += Def.task {
        val logger = streams.value.log
        val flatcBin = flatcExecutable.value.getAbsolutePath

        val cachedGen = FileFunction.cached(streams.value.cacheDirectory / "fbs") { (in: Set[File]) =>
          val inFiles: List[String] = flatcSources.value
            .flatMap(srcFolder => (srcFolder ** "*.fbs").get)
            .map(_.getAbsolutePath)
            .toList
          val outFolder = flatcOutput.value
          logger.info(s"Generating flatbuffers code")
          IO.delete(outFolder)
          val args: List[String] = flatcOptions.value.toList ++ ("-o" :: outFolder.getAbsolutePath :: inFiles)
          logger.debug(s"Running '$flatcBin ${args.mkString(" ")}'")
          val exitCode = sys.process.Process(flatcBin, args) ! logger
          if (exitCode != 0) throw new sbt.internal.util.MessageOnlyException("Could not generate FlatBuffers classes")
          (outFolder ** "*.java").get.toSet
        }

        cachedGen(flatcSources.value.toSet).toSeq
      },
      Compile / managedSourceDirectories += flatcOutput.value,
      libraryDependencies += "com.google.flatbuffers" % "flatbuffers-java" % flatbuffersV,
    )

}
