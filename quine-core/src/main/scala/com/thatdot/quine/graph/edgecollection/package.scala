package com.thatdot.quine.graph

import akka.NotUsed
import akka.stream.scaladsl.Source

package object edgecollection {
  type Identity[A] = A
  type NoMatSource[A] = Source[A, NotUsed]
}
