import scala.concurrent.duration.*
import scala.sys.process.*

import sbt.*
import sbt.Keys.{baseDirectory, name, streams, target, version}
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
    val includeNginx = docker / settingKey[Boolean]("Whether to install and use nginx in app container")
    val dockerJarTask = docker / taskKey[File]("The JAR file to include in the Docker image")
    val dockerStage = docker / taskKey[File]("Stage docker context without building the image")
  }
  import autoImport.*
  override lazy val projectSettings = Seq(
    dockerVolume := file("/var/quine"),
    dockerTags := sys.props.get("docker.tag").fold(Seq(version.value, "latest"))(Seq(_)),
    docker / imageNames := dockerTags.value.map(t =>
      ImageName(namespace = Some("thatdot"), repository = name.value, tag = Some(t)),
    ),
    docker / includeNginx := true,
    // Enforce Docker image format rather than OCI format (the Podman default), enabling HEALTHCHECK
    docker / dockerBuildArguments := Map("format" -> "docker"),
    // Default docker jar task - projects can override this to use packageObfuscatedJar
    docker / dockerJarTask := assembly.value,
    docker / dockerfile := {
      val jar: sbt.File = dockerJarTask.value

      val jarPath = "/" + jar.name
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
            tag = Some("21_35-jre-jammy"),
          ),
        )
        healthCheckShell(
          "curl --silent --fail http://localhost:8080/api/v1/admin/liveness || exit 1".split(' '),
          interval = Some(10.seconds),
          timeout = Some(2.seconds),
          startPeriod = Some(5.seconds),
        )
        expose(7626, 8080)
        env("QUINE_DATA", dockerVolume.value.getPath)
        volume("$QUINE_DATA")
        copy(jar, jarPath)
        copy(jmxPrometheusFile, jmxPrometheusJarName)
        copy(exporterYamlFile, exporterYamlPath)
      }
      // Do not include NGINX for Quine OSS
      if (includeNginx.value && name.value != "quine") {
        val quinePlusRootDir = baseDirectory.value.getParentFile
        val initScriptName = "init-quine.sh"
        val initScript = quinePlusRootDir / s"docker/$initScriptName"
        val initScriptDest = s"/$initScriptName"
        val nginxConfName = "nginx.conf.template"
        val nginxConf = quinePlusRootDir / s"docker/$nginxConfName"
        val nginxDest = s"/etc/nginx/$nginxConfName"
        val uid = 777
        val permissionsFix = s""" chown -R $uid:0 /var/log/nginx \\
                                | && chmod -R g+w /var/log/nginx \\
                                | && chown -R $uid:0 /var/lib/nginx \\
                                | && chmod -R g+w /var/lib/nginx \\
                                | && chown -R $uid:0 /etc/nginx \\
                                | && chmod -R g+w /etc/nginx""".stripMargin
        base
          .runRaw("apt-get update; apt-get install -y nginx")
          .runRaw("rm /etc/nginx/sites-enabled/default")
          .runRaw(permissionsFix)
          .copy(initScript, initScriptDest)
          .copy(nginxConf, nginxDest)
          .entryPoint(initScriptDest)
          .env("QUINE_JAR", jarPath)
      } else {
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
      }
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
