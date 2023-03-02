package com.thatdot.quine.graph

import akka.NotUsed
import akka.stream.scaladsl.Source

package object edges {
  type Identity[A] = A
  type NoMatSource[A] = Source[A, NotUsed]
}
