import sbt._
import sbt.io.IO
import sbt.Keys.{name, version}
import sbtassembly.AssemblyPlugin
import sbtassembly.AssemblyKeys.assembly
import sbtdocker.{DockerPlugin, Dockerfile, ImageName}
import sbtdocker.DockerKeys.{docker, dockerfile, imageNames}

import scala.concurrent.duration._
import scala.sys.process._

object Docker extends AutoPlugin {

  override def requires = AssemblyPlugin && DockerPlugin
  override def trigger = allRequirements

  object autoImport {
    // See https://github.com/marcuslonnberg/sbt-docker#pushing-an-image
    val dockerTags = SettingKey[Seq[String]]("docker-tags", "The tag names to push the docker image under")
    val dockerVolume = SettingKey[File]("docker-volume", "Path to where the app should save its data")
  }
  import autoImport._
  override lazy val projectSettings = Seq(
    dockerVolume := file("/var/quine"),
    dockerTags := sys.props.get("docker.tag").fold(Seq(version.value, "latest"))(Seq(_)),
    docker / imageNames := dockerTags.value.map(t =>
      ImageName(namespace = Some("thatdot"), repository = name.value, tag = Some(t)),
    ),
    docker / dockerfile := {
      val jar: sbt.File = assembly.value
      val jarPath = "/" + jar.name
      val jmxPrometheusJarName = "jmx_prometheus_javaagent.jar"
      val temp = IO.createTemporaryDirectory
      val jmxPrometheusFile: sbt.File = temp / "jmx_prometheus_javaagent.jar"
      url(
        "https://github.com/prometheus/jmx_exporter/releases/download/1.1.0/jmx_prometheus_javaagent-1.1.0.jar",
      ) #> jmxPrometheusFile !
      val jmxPrometheusJavaAgentPath = "/" + jmxPrometheusJarName
      val exporterYamlName = "exporter.yaml"
      val exporterYamlFile = temp / exporterYamlName
      IO.append(exporterYamlFile, "rules:\n- pattern: \".*\"")
      val exporterYamlPath = "/" + exporterYamlName
      new Dockerfile {
        from(
          ImageName(
            repository = "eclipse-temurin",
            tag = Some("21_35-jre-jammy"),
          ),
        )
        expose(8080, 7626)
        healthCheckShell(
          "curl --silent --fail http://localhost:8080/api/v1/admin/liveness || exit 1".split(' '),
          interval = Some(10.seconds),
          timeout = Some(2.seconds),
          startPeriod = Some(5.seconds),
        )
        env("QUINE_DATA", dockerVolume.value.getPath)
        volume("$QUINE_DATA")
        entryPoint(
          "java",
          "-XX:+AlwaysPreTouch",
          "-XX:+UseParallelGC",
          "-XX:InitialRAMPercentage=40.0",
          "-XX:MaxRAMPercentage=80.0",
          "-jar",
          jarPath,
        )
        copy(jar, jarPath)
        copy(jmxPrometheusFile, jmxPrometheusJarName)
        copy(exporterYamlFile, exporterYamlPath)
      }
    },
  )
}
