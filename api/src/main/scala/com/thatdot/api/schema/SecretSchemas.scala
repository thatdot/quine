package com.thatdot.api.schema

import sttp.tapir.Schema

import com.thatdot.common.security.Secret

/** Tapir schemas for [[Secret]] values. */
object SecretSchemas {

  /** Schema that represents Secret as a string in OpenAPI.
    *
    * The schema maps Secret to/from String using Secret.apply for creation
    * and Secret.toString for serialization (which redacts the value).
    */
  implicit val secretSchema: Schema[Secret] =
    Schema.string.map((s: String) => Some(Secret(s)))(_.toString)
}
