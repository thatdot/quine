package com.thatdot.quine.app.importers.util

import scala.reflect.{ClassTag, classTag}

import com.typesafe.scalalogging.LazyLogging
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  DefaultCredentialsProvider,
  StaticCredentialsProvider
}
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder
import software.amazon.awssdk.regions.Region

import com.thatdot.quine.routes.AwsCredentials

case object AwsOps extends LazyLogging {
  // the maximum number of simultaneous API requests any individual AWS client should make
  // invariant: all AWS clients using HTTP will set this as a maximum concurrency value
  val httpConcurrencyPerClient = 100

  def buildWith[Client: ClassTag, Builder <: AwsClientBuilder[Builder, Client]](
    builder: AwsClientBuilder[Builder, Client],
    credsOpt: Option[AwsCredentials]
  ): Builder =
    credsOpt.fold {
      logger.info(
        s"No AWS credentials provided while building AWS client of type ${classTag[Client].runtimeClass.getSimpleName}. Defaulting to environmental credentials."
      )
      builder.credentialsProvider(DefaultCredentialsProvider.create())
    } { credentials =>
      builder
        .credentialsProvider(
          StaticCredentialsProvider.create(
            AwsBasicCredentials.create(credentials.accessKeyId, credentials.secretAccessKey)
          )
        )
        .region(Region.of(credentials.region))
    }

  implicit class AwsBuilderOps[Client: ClassTag, Builder <: AwsClientBuilder[Builder, Client]](
    builder: AwsClientBuilder[Builder, Client]
  ) {

    /** Credentials to use for this AWS client. If provided, these will be used explicitly.
      * If absent, credentials will be inferred from the environment accoridng to AWS's DefaultCredentialsProvider
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
    def credentials(credsOpt: Option[AwsCredentials]): Builder = buildWith(builder, credsOpt)
  }
}
