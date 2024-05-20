import sbt._
import sbt.Keys.{name, version}
import sbtassembly.AssemblyPlugin
import sbtassembly.AssemblyKeys.assembly
import sbtdocker.{DockerPlugin, Dockerfile, ImageName}
import sbtdocker.DockerKeys.{docker, dockerfile, imageNames}

import scala.concurrent.duration._

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
      ImageName(namespace = Some("thatdot"), repository = name.value, tag = Some(t))
    ),
    docker / dockerfile := {
      val jar: sbt.File = assembly.value
      val jarPath = "/" + jar.name
      new Dockerfile {
        /* Our public mirror of this docker image from docker hub to avoid:
         * "toomanyrequests: You have reached your pull rate limit. You may increase the limit by authenticating and upgrading: https://www.docker.com/increase-rate-limit"
         * Published by docker/update_cached_jvm_base_image.sh in our opstools repo to https://us-west-2.console.aws.amazon.com/ecr/repositories/public/507566592123/eclipse-temurin
         */
        from(
          ImageName(
            registry = Some("public.ecr.aws"),
            namespace = Some("thatdot"),
            repository = "eclipse-temurin",
            tag = Some("21_35-jre-jammy")
          )
        )
        expose(8080, 7626)
        healthCheckShell(
          "curl --silent --fail http://localhost:8080/api/v1/admin/liveness || exit 1".split(' '),
          interval = Some(10.seconds),
          timeout = Some(2.seconds),
          startPeriod = Some(5.seconds)
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
          jarPath
        )
        copy(jar, jarPath)
      }
    }
  )
}
