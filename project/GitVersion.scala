import sbt.{AutoPlugin, SettingKey}
import sbt.Keys.version

import com.github.sbt.git.SbtGit.GitKeys.gitReader

object GitVersion extends AutoPlugin {

  override def trigger = allRequirements

  object autoImport {
    val tagPrefix = SettingKey[String]("tag-prefix", "The prefix of the git tag to use as the version number")
  }
  import autoImport._

  override lazy val projectSettings = Seq(
    tagPrefix := GitVersionPrefix.string,
    version := {
      val prefix = tagPrefix.value
      gitReader.value.withGit(_.describedVersion(Seq(prefix + '*'))).fold("UNKNOWN")(_.stripPrefix(prefix))
    }
  )

}
