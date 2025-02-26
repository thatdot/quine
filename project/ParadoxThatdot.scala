import com.github.sbt.paradox.material.theme.ParadoxMaterialThemePlugin
import com.lightbend.paradox.sbt.ParadoxPlugin
import com.typesafe.sbt.site.SitePreviewPlugin
import com.typesafe.sbt.site.paradox.ParadoxSitePlugin
import com.typesafe.sbt.web.Import.WebKeys
import com.typesafe.sbt.web.SbtWeb

import java.util.Calendar
import sbt.Keys.*
import sbt.*

// thatDot-themed paradox site
object ParadoxThatdot extends AutoPlugin {

  override def requires =
    ParadoxMaterialThemePlugin && ParadoxPlugin && ParadoxSitePlugin && SitePreviewPlugin
  override def trigger = noTrigger

  object autoImport {
    val projectName = settingKey[String]("name of the project on the site")
    val overlayDirectory = settingKey[File]("directory containing shared overlays")
    val templateDirectory = settingKey[File]("directory containing template overrides")
  }

  import autoImport.*
  import ParadoxPlugin.autoImport.*
  import ParadoxMaterialThemePlugin.autoImport.*
  import ParadoxSitePlugin.autoImport.*

  override lazy val projectSettings = inConfig(Compile)(
    Seq(
      paradoxMaterialTheme ~= {
        _.withoutFont()
          .withLogo("assets/images/logo.svg")
          .withFavicon("assets/images/favicon.svg")
          .withCopyright(s"© ${Calendar.getInstance.get(Calendar.YEAR)} thatDot, Inc.")
          .withGoogleAnalytics("UA-148518730-1")
      },
      paradoxProperties ++= Map(
        "project.name" -> projectName.value,
      ),
      templateDirectory := overlayDirectory.value / "_template",
      paradoxOverlayDirectories := Seq(overlayDirectory.value),
      paradoxTheme / sourceDirectories += templateDirectory.value,
      paradoxTheme / WebKeys.deduplicators += SbtWeb.selectFileFrom(templateDirectory.value),
      // For included MD files (see <https://github.com/lightbend/paradox/issues/350>)
      Compile / paradoxMarkdownToHtml / excludeFilter := {
        (Compile / paradoxMarkdownToHtml / excludeFilter).value ||
        ParadoxPlugin.InDirectoryFilter((Compile / paradox / sourceDirectory).value / "includes")
      },
    ),
  )
}
