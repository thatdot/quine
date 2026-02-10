package com.thatdot.quine.app.config

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import org.apache.pekko.util.Timeout

import pureconfig.BasicReaders.stringConfigReader
import pureconfig.error.CannotConvert
import pureconfig.generic.ProductHint
import pureconfig.generic.semiauto.{deriveConvert, deriveEnumerationConvert}
import pureconfig.{ConfigConvert, ConfigReader, ConfigWriter}

import com.thatdot.common.logging.Log.{LogConfig, RedactHide, RedactMethod}
import com.thatdot.quine.persistor.{EventEffectOrder, PersistenceConfig, PersistenceSchedule}
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

  implicit val persistenceConfigConvert: ConfigConvert[PersistenceConfig] =
    deriveConvert[PersistenceConfig]

  // RedactMethod is a sealed trait with only RedactHide case object
  // Uses type discriminator (e.g., redactor { type = redact-hide })
  implicit val redactHideConvert: ConfigConvert[RedactHide.type] = deriveConvert[RedactHide.type]
  implicit val redactMethodConvert: ConfigConvert[RedactMethod] = deriveConvert[RedactMethod]

  implicit val logConfigConvert: ConfigConvert[LogConfig] =
    deriveConvert[LogConfig]

  implicit val symbolConvert: ConfigConvert[Symbol] =
    ConfigConvert[String].xmap(Symbol(_), _.name)

  implicit val hostConvert: ConfigConvert[Host] =
    ConfigConvert[String].xmap(s => Host(replaceHostSpecialValues(s)), _.asString)
  implicit val portConvert: ConfigConvert[Port] =
    ConfigConvert[Int].xmap(i => Port(replacePortSpecialValue(i)), _.asInt)

  import software.amazon.awssdk.regions.Region
  private val regions = Region.regions.asScala.map(r => r.id -> r).toMap
  implicit val regionReader: ConfigReader[Region] = ConfigReader.fromNonEmptyString(s =>
    regions.get(s.toLowerCase) toRight CannotConvert(s, "Region", "expected one of " + regions.keys.mkString(", ")),
  )
  implicit val regionWriter: ConfigWriter[Region] = ConfigWriter.toString(_.id)
}
