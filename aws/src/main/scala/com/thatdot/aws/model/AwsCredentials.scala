package com.thatdot.aws.model

import com.thatdot.common.security.Secret

final case class AwsCredentials(accessKeyId: Secret, secretAccessKey: Secret)
