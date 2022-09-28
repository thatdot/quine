package com.thatdot.quine.app.config

import scala.concurrent.duration.FiniteDuration

import akka.util.Timeout

import pureconfig.ConfigConvert
import pureconfig.generic.ProductHint
import pureconfig.generic.semiauto.deriveEnumerationConvert

import com.thatdot.quine.persistor.{EventEffectOrder, PersistenceSchedule}

/** Collection of implicits for helping implicit resolution of pureconfig schemas
  */
object Implicits {

  // Unknown keys should be errors
  implicit def sealedProductHint[T]: ProductHint[T] = ProductHint[T](allowUnknownKeys = false)

  implicit val timeoutConvert: ConfigConvert[Timeout] = ConfigConvert[FiniteDuration].xmap(Timeout(_), _.duration)

  implicit val persistenceScheduleConvert: ConfigConvert[PersistenceSchedule] =
    deriveEnumerationConvert[PersistenceSchedule]

  implicit val effectOrderConvert: ConfigConvert[EventEffectOrder] =
    deriveEnumerationConvert[EventEffectOrder]
}
