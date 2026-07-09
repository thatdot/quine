package com.thatdot.quine.util

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.pekko.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import org.apache.pekko.stream.{Attributes, FlowShape, Inlet, Outlet}

object PressureGauge {
  trait State {
    def name: String
    def isUnderPressure: Boolean
  }

  def apply[A](): PressureGauge[A] =
    new PressureGauge[A](None)

  /** Create a self-registering gauge that registers its state with the given registry on materialization
    * and deregisters on stream stop.
    */
  def apply[A](registry: PressureGaugeRegistry, key: GaugeKey): PressureGauge[A] =
    new PressureGauge[A](Some((registry, key)))
}

class PressureGauge[A](registration: Option[(PressureGaugeRegistry, GaugeKey)])
    extends GraphStageWithMaterializedValue[FlowShape[A, A], PressureGauge.State] {

  val in: Inlet[A] = Inlet[A]("PressureGauge.in")
  val out: Outlet[A] = Outlet[A]("PressureGauge.out")
  override val shape: FlowShape[A, A] = FlowShape.of(in, out)

  override def createLogicAndMaterializedValue(attr: Attributes): (GraphStageLogic, PressureGauge.State) = {
    val isBackpressured = new AtomicBoolean(true)

    val logic = new GraphStageLogic(shape) {
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            isBackpressured.set(true)
            push(out, grab(in))
          }
        },
      )
      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            isBackpressured.set(false)
            pull(in)
          }
        },
      )

      override def postStop(): Unit =
        registration.foreach { case (registry, key) => registry.deregister(key) }
    }

    val state = new PressureGauge.State {
      override def isUnderPressure: Boolean = isBackpressured.get()
      override val name = attr.get[Attributes.Name](Attributes.Name("pressureGauge")).n
    }

    registration.foreach { case (registry, key) => registry.register(key, state) }

    (logic, state)
  }
}
