/*
 * Copyright 2016 Lightbend Inc. [http://www.lightbend.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *     [http://www.apache.org/licenses/LICENSE-2.0]
 *
 * This file has been modified under the terms of the Apache 2.0 license from the original file available at:
 *  https://github.com/akka/akka-stream-contrib/blob/0c48f32e1f004b3838e1862e45941759971cd193/src/main/scala/akka/stream/contrib/Valve.scala
 *
 * Copyright 2023 thatDot Inc.
 */

package com.thatdot.quine.util

import scala.concurrent.{Future, Promise}

import org.apache.pekko.stream._
import org.apache.pekko.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}

import com.thatdot.quine.util.SwitchMode.{Close, Open}

/** Pause/ Resume a Flow
  */
sealed trait ValveSwitch {

  /** Change the state of the valve
    *
    * @param mode expected mode to switch on
    * @return A future that completes with true if the mode did change and false if it already was in the requested mode
    */
  def flip(mode: SwitchMode): Future[Boolean]

  /** Obtain the state of the valve
    *
    * @return A future that completes with [[SwitchMode]] to indicate the current state of the valve
    */
  def getMode(): Future[SwitchMode]
}

object Valve {

  /** Factory for [[Valve]] instances.
    */
  def apply[A](): Valve[A] = Valve[A](SwitchMode.Open)

  /** Java API: Factory for [[Valve]] instances.
    */
  def create[A](): Valve[A] = Valve[A](SwitchMode.Open)

  /** Factory for [[Valve]] instances.
    */
  def apply[A](mode: SwitchMode): Valve[A] = new Valve[A](mode)

  /** Java API: Factory for [[Valve]] instances.
    */
  def create[A](mode: SwitchMode): Valve[A] = Valve[A](mode)

}

/** Materializes into a [[Future]] of [[ValveSwitch]] which provides a method, `flip`, that stops or restarts the flow of elements passing through the stage. As long as the valve is closed it will backpressure.
  *
  * Note that closing the valve could result in one element being buffered inside the stage, and if the stream completes or fails while being closed, that element may be lost.
  *
  * @param mode state of the valve at the startup of the flow (by default Open)
  */
final class Valve[A](mode: SwitchMode) extends GraphStageWithMaterializedValue[FlowShape[A, A], Future[ValveSwitch]] {

  val in: Inlet[A] = Inlet[A]("valve.in")

  val out: Outlet[A] = Outlet[A]("valve.out")

  override val shape: FlowShape[A, A] = FlowShape(in, out)

  override def createLogicAndMaterializedValue(
    inheritedAttributes: Attributes,
  ): (GraphStageLogic, Future[ValveSwitch]) = {
    val logic = new ValveGraphStageLogic(shape, mode)
    (logic, logic.promise.future)
  }

  private class ValveGraphStageLogic(shape: Shape, var mode: SwitchMode)
      extends GraphStageLogic(shape)
      with InHandler
      with OutHandler {

    val promise: Promise[ValveSwitch] = Promise[ValveSwitch]()

    private val switch = new ValveSwitch {

      val flipCallback = getAsyncCallback[(SwitchMode, Promise[Boolean])] { case (flipToMode, promise) =>
        val succeed = mode match {
          case _ if flipToMode == mode => false

          case Open =>
            mode = SwitchMode.Close
            true

          case Close =>
            if (isAvailable(in)) {
              push(out, grab(in))
            } else if (isAvailable(out) && !hasBeenPulled(in)) {
              pull(in)
            }

            mode = SwitchMode.Open
            true
        }

        promise.success(succeed)
      }

      val getModeCallback = getAsyncCallback[Promise[SwitchMode]](_.success(mode))

      override def flip(flipToMode: SwitchMode): Future[Boolean] = {
        val promise = Promise[Boolean]()
        implicit val ec = materializer.executionContext
        flipCallback
          .invokeWithFeedback((flipToMode, promise))
          .flatMap(_ => promise.future)
      }

      override def getMode(): Future[SwitchMode] = {
        val promise = Promise[SwitchMode]()
        implicit val ec = materializer.executionContext
        getModeCallback
          .invokeWithFeedback(promise)
          .flatMap(_ => promise.future)
      }
    }

    setHandlers(in, out, this)

    override def onPush(): Unit =
      if (isOpen) {
        push(out, grab(in))
      }

    override def onPull(): Unit =
      if (isOpen) {
        pull(in)
      }

    private def isOpen = mode == SwitchMode.Open

    override def preStart(): Unit =
      promise.success(switch)
  }

}

sealed trait SwitchMode

object SwitchMode {

  case object Open extends SwitchMode

  case object Close extends SwitchMode
}
