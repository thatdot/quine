package scala

package object compat {
  implicit class ImmutableMapOps[K, V](map: scala.collection.immutable.Map[K, V]) {
    // defined on 2.13 but not 2.12
    def updatedWith[V1 >: V](key: K)(remappingFunction: (Option[V]) => Option[V1]): Map[K, V1] =
      remappingFunction(map.get(key)) match {
        case Some(value) => map.updated(key, value)
        case None => map - key
      }
  }
  implicit class MutableMapOps[K, V](map: scala.collection.mutable.Map[K, V]) {
    // defined on 2.13 but not 2.12
    def updateWith(key: K)(remappingFunction: (Option[V]) => Option[V]): Option[V] = {
      val newEntry = remappingFunction(map.get(key))
      newEntry match {
        case Some(value) =>
          map.update(key, value)
        case None =>
          map.remove(key)
      }
      newEntry
    }
  }
}
