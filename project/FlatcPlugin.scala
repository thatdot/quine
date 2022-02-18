import sbt._
import sbt.Keys._
import sbt.util.CacheImplicits._
import scala.util.Properties

import java.net.URL
import java.nio.channels.{Channels, FileChannel};
import java.nio.file._
import java.util.zip._

object FlatcPlugin extends AutoPlugin {

  object autoImport {
    val flatcOptions = SettingKey[Seq[String]]("flatc-options", "Additional options to be passed to flatc")

    val flatcSources = SettingKey[Seq[File]]("flatc-sources", "Directories to look for source files")

    val flatcOutput = SettingKey[File]("flatc-output", "Directory into which outputs will be written")

    val flatcDependency = SettingKey[Option[URL]]("flatc-dependency", "URL for zipped binary artifact for flatc")

    val flatcExecutable = TaskKey[File](
      "flatc-executable",
      "Path to a flatc executable. Default downloads flatcDependency from Github."
    )
  }

  import autoImport._

  override def projectSettings: Seq[Def.Setting[_]] =
    Seq(
      flatcOptions := Seq("--java"),
      flatcSources := Seq((Compile / sourceDirectory).value / "fbs"),
      flatcOutput := (Compile / sourceManaged).value / "fbs",
      flatcDependency := {
        val prefix = "https://github.com/google/flatbuffers/releases/download/v2.0.0/"
        val suffixOpt =
          if (Properties.isMac) Some("Mac.flatc.binary.zip")
          else if (Properties.isWin) Some("Windows.flatc.binary.zip")
          else if (Properties.isLinux) Some("Linux.flatc.binary.clang++-9.zip")
          else None

        suffixOpt.map(suffix => new URL(prefix + suffix))
      },
      flatcExecutable := {
        val outputDirectory = (Compile / target).value / "flatc"
        val url: URL = flatcDependency.value.getOrElse {
          val os = System.getProperty("os.name")
          val suggestion = "set flatcExecutable := file(path-to-flatc)"
          throw new sbt.internal.util.MessageOnlyException(
            s"Could not identify flatc binary for $os (try manually setting `$suggestion`)"
          )

        }
        val flatcStore = streams.value.cacheStoreFactory.make("flatcStore")

        /* Fetch the right `flatc` binary
         *
         * @param file directory into which to place the `flatc` binary
         * @param url URL from which to download a ZIP of the `flatc` binary
         * @return path to the downloaded flatc
         */
        val getFlatc: Tuple2[File, URL] => File = Cache.cached[(File, URL), File](flatcStore) {
          case (outputDirectory, url) =>
            val logger = streams.value.log

            var zipIS: ZipInputStream = null
            var fileChannel: FileChannel = null
            var toPath: Path = null

            try {
              logger.info(s"Downloading flatc from $url...")
              zipIS = new ZipInputStream(Channels.newInputStream(Channels.newChannel(url.openStream())))
              val entry = zipIS.getNextEntry
              toPath = outputDirectory.toPath.resolve(entry.getName())
              IO.createDirectory(outputDirectory)
              fileChannel = FileChannel.open(toPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
              fileChannel.transferFrom(Channels.newChannel(zipIS), 0, Long.MaxValue)
              if (IO.isPosix) IO.chmod("rwxr--r--", toPath.toFile)
              logger.info(s"Saved flatc to $toPath")
            } finally {
              if (zipIS != null) zipIS.close()
              if (fileChannel != null) fileChannel.close()
            }

            toPath.toFile
        }

        getFlatc(outputDirectory, url)

      },
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
          val exitCode = scala.sys.process.Process(flatcBin, args) ! logger
          if (exitCode != 0) throw new sbt.internal.util.MessageOnlyException("Could not generate FlatBuffers classes")
          (outFolder ** "*.java").get.toSet
        }

        cachedGen(flatcSources.value.toSet).toSeq
      },
      libraryDependencies += "com.google.flatbuffers" % "flatbuffers-java" % Dependencies.flatbuffersV
    )

}
