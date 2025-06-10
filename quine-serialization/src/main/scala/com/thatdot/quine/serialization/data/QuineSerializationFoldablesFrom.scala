package com.thatdot.quine.serialization.data

import com.thatdot.data.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.model.{QuineIdProvider, QuineValue}

object QuineSerializationFoldablesFrom {
  implicit def quineValueDataFoldableFrom(implicit idProvider: QuineIdProvider): DataFoldableFrom[QuineValue] =
    new DataFoldableFrom[QuineValue] {
      override def fold[B](value: QuineValue, folder: DataFolderTo[B]): B = value match {
        case QuineValue.Str(string) => folder.string(string)
        case QuineValue.Integer(long) => folder.integer(long)
        case QuineValue.Floating(double) => folder.floating(double)
        case QuineValue.True => folder.trueValue
        case QuineValue.False => folder.falseValue
        case QuineValue.Null => folder.nullValue
        case QuineValue.Bytes(bytes) => folder.bytes(bytes)
        case QuineValue.List(list) =>
          val builder = folder.vectorBuilder()
          list.foreach(qv => builder.add(fold(qv, folder)))
          builder.finish()
        case QuineValue.Map(map) =>
          val builder = folder.mapBuilder()
          map.foreach { case (k, v) =>
            builder.add(k, fold(v, folder))
          }
          builder.finish()
        case QuineValue.DateTime(instant) => folder.localDateTime(instant.toLocalDateTime)
        case QuineValue.Duration(duration) => folder.duration(duration)
        case QuineValue.Date(date) => folder.date(date)
        case QuineValue.LocalTime(time) => folder.localTime(time)
        case QuineValue.Time(time) => folder.time(time)
        case QuineValue.LocalDateTime(localDateTime) => folder.localDateTime(localDateTime)
        case QuineValue.Id(id) => folder.string(idProvider.qidToPrettyString(id))
      }
    }
}
