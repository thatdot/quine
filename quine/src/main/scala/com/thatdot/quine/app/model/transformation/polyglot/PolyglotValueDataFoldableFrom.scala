package com.thatdot.quine.app.model.transformation.polyglot

import java.time._

import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.graalvm.polyglot

import com.thatdot.data.{DataFoldableFrom, DataFolderTo}

/** Implementation of [[DataFoldableFrom]] for GraalVM's [[org.graalvm.polyglot.Value]].
  *
  * The goal is to walk a guest-language value and rebuild it in the target produced
  * by [[DataFolderTo]]. The program is a big pattern-match ordered from most to least
  * specific. Order does matter.
  */
object PolyglotValueDataFoldableFrom extends DataFoldableFrom[polyglot.Value] {
  // Regex used to detect whether a numeric literal *looks* like a floating‑point.
  // If the string contains "." or an exponent (e/E) we keep it as a Double; otherwise
  // – provided it fits in Long – we map it to an integral value.
  // This is necessary for numbers like: 1.9365476157539434e17 as this fits in a long and will take precedence but
  // the host value intended this as a double
  private val hasDecimalOrExponent = "[.eE]".r

  /** Fold a Polyglot value into the caller‑supplied folder.
    *
    * @param value   Graal VM guest value to inspect
    * @param folder  type‑class instance that knows how to build `B`
    */
  def fold[B](value: polyglot.Value, folder: DataFolderTo[B]): B = {
    value match {
      case _ if value.isNull => folder.nullValue
      case _ if value.isBoolean => if (value.asBoolean()) folder.trueValue else folder.falseValue

      // integral number: fits in Long and literal has no decimal/exponent part
      case _ if value.isNumber && value.fitsInLong && hasDecimalOrExponent.findFirstIn(value.toString).isEmpty =>
        folder.integer(value.asLong())
      case _ if value.isNumber && value.fitsInDouble => folder.floating(value.asDouble())
      case _ if value.isString => folder.string(value.asString())
      case _ if value.hasBufferElements =>
        val count = value.getBufferSize.toInt
        val bytes = Array.ofDim[Byte](count)
        // This is really inefficient. Later versions of graalvm add a bulk readBuffer operation that
        // fills a byte array. When we switch to only supporting Java 17+, which is required Graal 23.0+
        // this can be improved.
        for (i <- 0 until count)
          bytes(i) = value.readBufferByte(i.toLong)
        folder.bytes(bytes)
      case _ if value.isDate && value.isTime && value.isTimeZone =>
        folder.zonedDateTime(ZonedDateTime.ofInstant(value.asInstant, value.asTimeZone))
      case _ if value.isDate && value.isTime =>
        folder.localDateTime(LocalDateTime.of(value.asDate, value.asTime))
      case _ if value.isDate => folder.date(value.asDate)
      case _ if value.isTime && value.isTimeZone =>
        folder.time(OffsetTime.ofInstant(value.asInstant(), value.asTimeZone()))
      case _ if value.isTime => folder.localTime(value.asTime)
      case _ if value.isDuration => folder.duration(value.asDuration)

      // Any input that is produced by the [[PolyglotValueDataFolderTo]] will be a host object and not match the
      // above checks
      case _ if value.isHostObject =>
        value.asHostObject[Object]() match {
          case time: ZonedDateTime => folder.zonedDateTime(time)
          case time: LocalDateTime => folder.localDateTime(time)
          case time: LocalDate => folder.date(time)
          case time: OffsetTime => folder.time(time)
          case time: LocalTime => folder.localTime(time)
          case duration: Duration => folder.duration(duration)
          case bytes: Array[Byte] => folder.bytes(bytes)
          case _ => throw new Exception(s"host value $value of class ${value.getClass} not supported")
        }

      case _ if value.hasHashEntries =>
        val it = value.getHashEntriesIterator
        val builder = folder.mapBuilder()
        while (it.hasIteratorNextElement) {
          val entry = it.getIteratorNextElement
          val k = entry.getArrayElement(0)
          val v = entry.getArrayElement(1)
          builder.add(k.asString, fold(v, folder))
        }
        builder.finish()

      case _ if value.hasArrayElements =>
        val size = value.getArraySize
        val builder = folder.vectorBuilder()
        var i = 0L
        while (i < size) {
          val elem = value.getArrayElement(i)
          builder.add(fold(elem, folder))
          i += 1
        }
        builder.finish()

      case _ if value.hasIterator =>
        val it = value.getIterator
        val builder = folder.vectorBuilder()
        while (it.hasIteratorNextElement) {
          val elem = it.getIteratorNextElement
          builder.add(fold(elem, folder))
        }
        builder.finish()

      case _ if value.hasMembers =>
        val builder = folder.mapBuilder()

        for (key <- value.getMemberKeys.asScala) {
          val v = value.getMember(key)
          builder.add(key, fold(v, folder))
        }

        builder.finish()

      // Any input that is produced by the [[PolyglotValueDataFolderTo]] will have certain proxy objects that are
      // handled by the below, as well as any polyglot language that could produce a proxy object.
      case proxy if value.isProxyObject =>
        value.asProxyObject[polyglot.proxy.Proxy]() match {
          case array: polyglot.proxy.ProxyArray =>
            val size = array.getSize
            val builder = folder.vectorBuilder()
            var i = 0L
            while (i < size) {

              val elem = polyglot.Value.asValue(array.get(i))
              builder.add(fold(elem, folder))
              i += 1
            }
            builder.finish()

          case obj: polyglot.proxy.ProxyObject =>
            val builder = folder.mapBuilder()

            // The below cases come from the definition of getMemberKeys as to what it's type could be.
            obj.getMemberKeys match {

              case null => () // Do nothing in the case that there are no members
              case arr: polyglot.proxy.ProxyArray =>
                val size = arr.getSize
                for (i <- 0L until size) {
                  val key = arr.get(i).toString
                  val v = obj.getMember(key)
                  builder.add(key, fold(polyglot.Value.asValue(v), folder))
                }

              case keys: List[_] =>
                keys.foreach { key =>
                  // If this case matches graal vm asserts this is true.  If it's not there is probably a bug so fail.
                  // This has been tested
                  assert(key.isInstanceOf[String])
                  val v = obj.getMember(key.toString)
                  builder.add(key.toString, fold(polyglot.Value.asValue(v), folder))
                }

              case keys: Array[String] =>
                keys.foreach { key =>
                  val v = obj.getMember(key)
                  builder.add(key, fold(polyglot.Value.asValue(v), folder))
                }
              case _ =>
                throw new Exception(s"value $proxy of class ${proxy.getClass} not supported")
            }
            builder.finish()

          case _ =>
            throw new Exception(s"value $proxy of class ${proxy.getClass} not supported")

        }

      case other =>
        throw new Exception(s"value $other of class ${other.getClass} not supported")
    }
  }

}
