import scala.concurrent.duration.*
import scala.sys.process.*

import sbt.*
import sbt.Keys.{name, streams, target, version}
import sbt.io.IO
import sbtassembly.AssemblyKeys.assembly
import sbtassembly.AssemblyPlugin
import sbtdocker.DockerKeys.{docker, dockerBuildArguments, dockerfile, imageNames}
import sbtdocker.staging.DefaultDockerfileProcessor
import sbtdocker.{DockerPlugin, Dockerfile, DockerfileLike, ImageName}

object Docker extends AutoPlugin {

  override def requires = AssemblyPlugin && DockerPlugin
  override def trigger = allRequirements

  object autoImport {
    // See https://github.com/marcuslonnberg/sbt-docker#pushing-an-image
    val dockerTags = SettingKey[Seq[String]]("docker-tags", "The tag names to push the docker image under")
    val dockerVolume = SettingKey[File]("docker-volume", "Path to where the app should save its data")
    val dockerJarTask = docker / taskKey[File]("The JAR file to include in the Docker image")
    val dockerDistDir = docker / taskKey[Option[File]](
      "Staged distribution with <name>.jar and lib/, copied to /opt/<name>/ in two layers; None for a self-contained JAR",
    )
    val dockerStage = docker / taskKey[File]("Stage docker context without building the image")
  }
  import autoImport.*
  override lazy val projectSettings = Seq(
    dockerVolume := file("/var/quine"),
    dockerTags := sys.props.get("docker.tag").fold(Seq(version.value, "latest"))(Seq(_)),
    docker / imageNames := dockerTags.value.map(t =>
      ImageName(namespace = Some("thatdot"), repository = name.value, tag = Some(t)),
    ),
    // Enforce Docker image format rather than OCI format (the Podman default), enabling HEALTHCHECK
    docker / dockerBuildArguments := Map("format" -> "docker"),
    // Quine OSS ships its fat `assembly` JAR; `ThinDist.settings` sets `dockerDistDir` for the commercial products
    docker / dockerJarTask := assembly.value,
    docker / dockerDistDir := None,
    docker / dockerfile := {
      val product = name.value
      val dist: Option[sbt.File] = dockerDistDir.value
      val assemblyJar: sbt.File = dockerJarTask.value
      val jar: sbt.File = dist.fold(assemblyJar)(_ / s"$product.jar")
      val lib: Option[sbt.File] = dist.map(_ / "lib")
      val appDir = if (dist.isDefined) s"/opt/$product" else ""
      val jarPath = s"$appDir/${jar.name}"
      val jmxPrometheusJarName = "jmx_prometheus_javaagent.jar"
      val temp = IO.createTemporaryDirectory
      val jmxPrometheusFile: sbt.File = temp / "jmx_prometheus_javaagent.jar"
      url(
        "https://github.com/prometheus/jmx_exporter/releases/download/1.1.0/jmx_prometheus_javaagent-1.1.0.jar",
      ) #> jmxPrometheusFile !
      val exporterYamlName = "exporter.yaml"
      val exporterYamlFile = temp / exporterYamlName
      IO.append(exporterYamlFile, "rules:\n- pattern: \".*\"")
      val exporterYamlPath = "/" + exporterYamlName
      val base = new Dockerfile {
        from(
          ImageName(
            repository = "eclipse-temurin",
            tag = Some("21.0.11_10-jre-noble"),
          ),
        )
        runRaw("apt-get update && apt-get -y upgrade && rm -rf /var/lib/apt/lists/*")
        healthCheckShell(
          "curl --silent --fail http://localhost:8080/api/v2/system/liveness || exit 1".split(' '),
          interval = Some(10.seconds),
          timeout = Some(2.seconds),
          startPeriod = Some(5.seconds),
        )
        expose(7626, 8080)
        env("QUINE_DATA", dockerVolume.value.getPath)
        volume("$QUINE_DATA")
        // Dependencies first, so a first-party change invalidates only the product JAR's layer
        lib.foreach(copy(_, s"$appDir/lib"))
        copy(jar, jarPath)
        copy(jmxPrometheusFile, jmxPrometheusJarName)
        copy(exporterYamlFile, exporterYamlPath)
      }
      base
        .entryPoint(
          "java",
          "-XX:+AlwaysPreTouch",
          "-XX:+UseParallelGC",
          "-XX:InitialRAMPercentage=40.0",
          "-XX:MaxRAMPercentage=80.0",
          "-jar",
          jarPath,
        )
    },
    dockerStage := {
      val log = streams.value.log
      val stageDir = target.value / "docker"
      val df = (docker / dockerfile).value.asInstanceOf[DockerfileLike]

      // Use sbt-docker's internal staging processor
      val staged = DefaultDockerfileProcessor(df, stageDir)

      // Clean and create stage directory
      IO.delete(stageDir)
      IO.createDirectory(stageDir)

      // Write Dockerfile
      IO.write(stageDir / "Dockerfile", staged.instructionsString)

      // Copy all staged files
      staged.stageFiles.foreach { case (source, dest) =>
        source.stage(dest)
      }

      log.info(s"Docker context staged to: $stageDir")
      stageDir
    },
  )
}
