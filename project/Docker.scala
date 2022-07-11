import sbt._
import sbt.Keys.{name, version}
import sbtassembly.AssemblyPlugin
import sbtassembly.AssemblyKeys.assembly
import sbtdocker.{DockerPlugin, Dockerfile, ImageName}
import sbtdocker.DockerKeys.{docker, dockerfile, imageNames}

import scala.concurrent.duration._

object Docker extends AutoPlugin {

  override def requires = AssemblyPlugin && DockerPlugin
  override def trigger = noTrigger

  object autoImport {
    // See https://github.com/marcuslonnberg/sbt-docker#pushing-an-image
    val dockerTags = SettingKey[Seq[String]]("docker-tags", "The tag names to push the docker image under")
  }
  import autoImport.dockerTags
  override lazy val projectSettings = Seq(
    dockerTags := Seq(version.value, "latest"),
    docker / imageNames := dockerTags.value.map(t =>
      ImageName(namespace = Some("thatdot"), repository = name.value, tag = Some(t))
    ),
    docker / dockerfile := {
      val jar: sbt.File = assembly.value
      val jarPath = "/" + jar.name
      new Dockerfile {
        /* Our public mirror of this docker image from docker hub to avoid:
         * "toomanyrequests: You have reached your pull rate limit. You may increase the limit by authenticating and upgrading: https://www.docker.com/increase-rate-limit"
         * To update this copy:
             docker pull adoptopenjdk:16-jre-openj9-focal
             aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/p0a2o6c9
             docker tag adoptopenjdk:16-jre-openj9-focal public.ecr.aws/p0a2o6c9/adoptopenjdk:16-jre-openj9-focal
             docker push public.ecr.aws/p0a2o6c9/adoptopenjdk:16-jre-openj9-focal
         */
        from("public.ecr.aws/p0a2o6c9/adoptopenjdk:16.0.1_9-jre-hotspot-focal")

        expose(8080)
        healthCheckShell(
          "curl --silent --fail http://localhost:8080/api/v1/admin/liveness || exit 1".split(' '),
          interval = Some(10.seconds),
          timeout = Some(2.seconds),
          startPeriod = Some(5.seconds)
        )
        env("DATA_DIR", "/var/thatdot")
        volume("$DATA_DIR")
        entryPoint(
          "java",
          "-XX:+AlwaysPreTouch",
          "-XX:+UseParallelGC",
          "-XX:InitialRAMPercentage=40.0",
          "-XX:MaxRAMPercentage=80.0",
          "-jar",
          jarPath
        )
        add(jar, jarPath)
      }
    }
  )
}
