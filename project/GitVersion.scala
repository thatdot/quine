import scala.util.Using
import sbt.{AutoPlugin, SettingKey}
import sbt.Keys.{baseDirectory, version}

import org.eclipse.jgit.lib.RepositoryBuilder
import org.eclipse.jgit.api.Git
import com.typesafe.sbt.SbtGit.GitKeys.gitReader

object GitVersion extends AutoPlugin {

  override def trigger = allRequirements

  object autoImport {
    val tagPrefix = SettingKey[String]("tag-prefix", "The prefix of the git tag to use as the version number")
  }
  import autoImport._

  override lazy val projectSettings = Seq(
    tagPrefix := "quine/",
    version := {
      val prefix = tagPrefix.value
      gitReader.value.withGit(_.describedVersion(Seq(prefix + '*'))).fold("UNKNOWN")(_.stripPrefix(prefix))
    }
  )

}
