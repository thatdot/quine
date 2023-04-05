package com.thatdot.quine.app.config

import scala.concurrent.duration.FiniteDuration

import akka.util.Timeout

import pureconfig.BasicReaders.stringConfigReader
import pureconfig.ConfigConvert
import pureconfig.generic.ProductHint
import pureconfig.generic.semiauto.deriveEnumerationConvert

import com.thatdot.quine.persistor.{EventEffectOrder, PersistenceSchedule}
import com.thatdot.quine.util.Config._
import com.thatdot.quine.util.{Host, Port}

/** Collection of implicits for helping implicit resolution of pureconfig schemas
  */
trait PureconfigInstances {

  // Unknown keys should be errors
  implicit def sealedProductHint[T]: ProductHint[T] = ProductHint[T](allowUnknownKeys = false)

  implicit val timeoutConvert: ConfigConvert[Timeout] = ConfigConvert[FiniteDuration].xmap(Timeout(_), _.duration)

  implicit val persistenceScheduleConvert: ConfigConvert[PersistenceSchedule] =
    deriveEnumerationConvert[PersistenceSchedule]

  implicit val effectOrderConvert: ConfigConvert[EventEffectOrder] =
    deriveEnumerationConvert[EventEffectOrder]

  implicit val symbolConvert: ConfigConvert[Symbol] =
    ConfigConvert[String].xmap(Symbol(_), _.name)

  implicit val hostConvert: ConfigConvert[Host] =
    ConfigConvert[String].xmap(s => Host(replaceHostSpecialValues(s)), _.asString)
  implicit val portConvert: ConfigConvert[Port] =
    ConfigConvert[Int].xmap(i => Port(replacePortSpecialValue(i)), _.asInt)
}
