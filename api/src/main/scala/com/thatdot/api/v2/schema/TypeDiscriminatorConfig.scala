package com.thatdot.api.v2.schema

trait TypeDiscriminatorConfig extends V2ApiConfiguration {
  implicit val config: Configuration = typeDiscriminatorConfig
}

object TypeDiscriminatorConfig extends TypeDiscriminatorConfig
