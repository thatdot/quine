import java.nio.charset.StandardCharsets.UTF_8
import sbt._
import sbt.Keys.streams
import sbtdocker.DockerKeys.{docker, imageNames}
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.ecr.EcrClient

import java.util.Base64
import scala.sys.process._

object Ecr extends AutoPlugin {
  object autoImport {
    val publishToEcr = SettingKey[Boolean]("publish-to-ecr", "Flag to enable publishing docker images to ECR")
    // Returns an Option in case e.g. the user doesn't have AWS creds
    val ecrLogin = TaskKey[Option[URL]]("ecr-login", "Login to ECR, returning the URL to the docker registry")
  }
  import autoImport._

  override def requires = Docker

  override lazy val projectSettings = Seq(
    publishToEcr := true,
    ecrLogin := (try {
      val authData = EcrClient.create.getAuthorizationToken.authorizationData.get(0)
      val authTokenString = new String(Base64.getDecoder.decode(authData.authorizationToken), UTF_8)
      val Array(user, pass) = authTokenString.split(':')
      val domain = authData.proxyEndpoint
      Seq("docker", "login", "--username", user, "--password-stdin", domain).run(stringToStdIn(pass))
      Some(new URL(domain))
    } catch {
      case e: SdkClientException => // E.g. no AWS creds in environment
        streams.value.log.warn("Unable to get ECR token: " + e.getMessage)
        None
    }),
    docker / imageNames := {
      val images = (docker / imageNames).value
      ecrLogin.value match {
        case Some(ecrRegistry) if publishToEcr.value => images.map(_.copy(registry = Some(ecrRegistry.getHost)))
        case _ => images
      }
    }
  )
  // Used to pipe the password to the `docker login` process
  private def stringToStdIn(s: String): ProcessIO = BasicIO.standard { os =>
    os.write(s.getBytes(UTF_8))
    os.close()
  }
}
