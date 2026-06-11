import sbt.{AutoPlugin, SettingKey}
import sbt.Keys.version
import com.github.sbt.git.SbtGit.GitKeys.gitReader
import com.github.sbt.git.GitReadonlyInterface

object GitVersion extends AutoPlugin {

  override def trigger = allRequirements

  object autoImport {
    val tagPrefix = SettingKey[String]("tag-prefix", "The prefix of the git tag to use as the version number")
  }
  import autoImport._

  private def tagWithPrefix(git: GitReadonlyInterface, prefix: String): Option[String] =
    git.describedVersion(Seq(prefix + '*')).map(_.stripPrefix(prefix))

  private def currentTagWithPrefix(git: GitReadonlyInterface, prefix: String): Option[String] =
    git.currentTags.find(_.startsWith(prefix)).map(_.stripPrefix(prefix))

  override lazy val projectSettings = Seq(
    tagPrefix := "quine/",
    version := gitReader.value.withGit(git =>
      currentTagWithPrefix(git, "prerelease/" + tagPrefix.value)
      orElse tagWithPrefix(git, tagPrefix.value)
      orElse tagWithPrefix(git, "v")
      orElse git.headCommitSha.map(sha => s"ManuallyCompiled-${sha.take(10)}")
      getOrElse "ManuallyCompiled",
    ),
  )

}
