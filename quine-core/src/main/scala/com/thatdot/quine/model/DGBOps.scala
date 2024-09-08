package com.thatdot.quine.model

object DGBOps {

  implicit final class DNMapOps(
    private val map: Map[Symbol, (PropertyComparisonFunc, Option[PropertyValue])],
  ) extends AnyVal {
    def containsByLeftConditions(other: Map[Symbol, (PropertyComparisonFunc, Option[PropertyValue])]): Boolean =
      other.forall { case (key, (_, v1)) =>
        map.get(key).exists { case (compFunc, v2) => compFunc(v1, v2) }
      }

    def containsByRightConditions(other: Map[Symbol, (PropertyComparisonFunc, Option[PropertyValue])]): Boolean =
      other.forall { case (key, (compFunc, v1)) =>
        map.get(key).exists { case (_, v2) => compFunc(v1, v2) }
      }

    def containsByBothConditions(other: Map[Symbol, (PropertyComparisonFunc, Option[PropertyValue])]): Boolean =
      other.forall { case (key, (compFunc1, v1)) =>
        map.get(key).exists { case (compFunc2, v2) =>
          compFunc1(v1, v2) &&
            compFunc2(v1, v2)
        }
      }
  }
}
