package com.thatdot.aws.util

import scala.reflect.{ClassTag, classTag}

import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  AwsCredentialsProvider,
  DefaultCredentialsProvider,
  StaticCredentialsProvider,
}
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder
import software.amazon.awssdk.regions.Region

import com.thatdot.aws.model._
import com.thatdot.common.logging.Log._

case object AwsOps extends LazySafeLogging {
  // the maximum number of simultaneous API requests any individual AWS client should make
  // invariant: all AWS clients using HTTP will set this as a maximum concurrency value
  val httpConcurrencyPerClient = 100

  def staticCredentialsProviderV2(credsOpt: Option[AwsCredentials]): AwsCredentialsProvider =
    credsOpt.fold[AwsCredentialsProvider](DefaultCredentialsProvider.builder.build) { credentials =>
      StaticCredentialsProvider.create(
        AwsBasicCredentials.create(credentials.accessKeyId, credentials.secretAccessKey),
      )
    }

  implicit class AwsBuilderOps[Client: ClassTag, Builder <: AwsClientBuilder[Builder, Client]](
    builder: AwsClientBuilder[Builder, Client],
  ) {

    /** Credentials to use for this AWS client. If provided, these will be used explicitly.
      * If absent, credentials will be inferred from the environment according to AWS's DefaultCredentialsProvider
      * This may have security implications! Ensure your environment only contains environment variables,
      * java system properties, aws credentials files, and instance profile credentials you trust!
      *
      * @see https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
      *
      * If you are deploying on EC2 and do NOT wish to use EC2 container metadata/credentials, ensure the java property
      * `aws.disableEc2Metadata` is set to true, or the environment variable AWS_EC2_METADATA_DISABLED is set to true.
      * Note that this will also disable region lookup, and thus require all AWS client constructions to explicitly set
      * credentials.
      *
      * @param credsOpt if set, aws credentials to use explicitly
      * @return
      */
    def credentialsV2(credsOpt: Option[AwsCredentials]): Builder = {
      val creds = credsOpt.orElse {
        logger.info(
          safe"""No AWS credentials provided while building AWS client of type
               |${Safe(classTag[Client].runtimeClass.getSimpleName)}. Defaulting
               |to environmental credentials.""".cleanLines,
        )
        None
      }
      builder.credentialsProvider(staticCredentialsProviderV2(creds))
    }

    def regionV2(regionOpt: Option[AwsRegion]): Builder =
      regionOpt.fold {
        logger.info(
          safe"""No AWS region provided while building AWS client of type:
                |${Safe(classTag[Client].runtimeClass.getSimpleName)}.
                |Defaulting to environmental settings.""".cleanLines,
        )
        builder.applyMutation(_ => ()) // return the builder unmodified
      }(region => builder.region(Region.of(region.region)))
  }
}
