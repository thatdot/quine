package com.thatdot.data

import java.time.{Duration => JavaDuration, LocalDate, LocalDateTime, LocalTime, OffsetTime, ZonedDateTime}

import scala.util.Random

/** Object including all types that are covered by [[DataFoldableFrom]] */
case class FoldableTestData(
  nullValue: Null = null,
  trueValue: Boolean = true,
  falseValue: Boolean = false,
  integerValue: Integer = Random.nextInt(),
  stringValue: String = Random.nextString(Random.nextInt(10)),
  bytesValue: Array[Byte] = Random.nextBytes(10),
  floatingValue: Double = Random.nextDouble(),
  dateValue: LocalDate = LocalDate.now(),
  timeValue: OffsetTime = OffsetTime.now(),
  localTimeValue: LocalTime = LocalTime.now(),
  localDateTimeValue: LocalDateTime = LocalDateTime.now(),
  zonedDateTimeValue: ZonedDateTime = ZonedDateTime.now(),
  durationValue: JavaDuration = JavaDuration.ofNanos(Random.between(0L, Long.MaxValue)),
  mapValue: Map[String, Any] = Map.empty[String, Any],
  vectorValue: Vector[Any] = Vector.empty[Any],
) {

  def asMap: Map[String, Any] =
    0.until(productArity).map(i => productElementName(i) -> productElement(i)).toMap

  def asVector: Vector[Any] = 0.until(productArity).map(i => productElement(i)).toVector

  def foldTo[B](implicit dataFolder: DataFolderTo[B]): B = {
    val mapBuilder = dataFolder.mapBuilder()
    asMap.foreach { case (k, v) => mapBuilder.add(k, FoldableTestData.fromAnyDataFoldable.fold(v, dataFolder)) }
    mapBuilder.finish()
  }
}

object FoldableTestData {
  val fromAnyDataFoldable: DataFoldableFrom[Any] = new DataFoldableFrom[Any] {
    override def fold[B](value: Any, folder: DataFolderTo[B]): B =
      value match {
        case null => folder.nullValue
        case true => folder.trueValue
        case false => folder.falseValue
        case s: String => folder.string(s)
        case b: Array[Byte] => folder.bytes(b)
        case i: Int => folder.integer(i.longValue())
        case l: Long => folder.integer(l)
        case d: Number => folder.floating(d.doubleValue())
        case ld: LocalDate => folder.date(ld)
        case ldt: LocalDateTime => folder.localDateTime(ldt)
        case t: OffsetTime => folder.time(t)
        case lt: LocalTime => folder.localTime(lt)
        case zdt: ZonedDateTime => folder.zonedDateTime(zdt)
        case dur: JavaDuration => folder.duration(dur)
        case m: Map[_, _] =>
          val b = folder.mapBuilder()
          m.foreach { case (key, value) => b.add(key.toString, fold(value, folder)) }
          b.finish()
        case c: Iterable[Any] =>
          val b = folder.vectorBuilder()
          c.foreach(v => b.add(fold(v, folder)))
          b.finish()

        case other => throw new UnsupportedOperationException(s" Value $other of type ${other.getClass} is not handled")
      }
  }

}
