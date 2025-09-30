package com.thatdot.aws.model

import org.polyvariant.sttp.oauth2.Secret

final case class AwsCredentials(accessKeyId: String, secretAccessKey: Secret[String])
