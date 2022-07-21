package com.thatdot.quine.util

import java.util.concurrent.atomic._

import scala.concurrent.{Future, Promise}

import akka.NotUsed
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Graph, Inlet, Outlet}

/** Valve which can be closed/opened from many different places at once.
  *
  * The valve can be closed multiple times over, in which case it will need to
  * be re-opened the same number of times again to count as opened.
  *
  * @param name name of the valve (for debug purposes)
  */
class SharedValve(val name: String) {

  /** Atomic state of the valve.
    *
    * The promise inside the valve is always `null` or pending. Whenever the
    * state is updated so that a new promise is included, the previous promise
    * must also be completed shortly after the state update.
    *
    * == Contention
    *
    * This state will be updated quite frequently (every time the valve
    * is opened or closed). Consequently, this reference may become highly
    * contended, at which point it may become a bottleneck. Consider using
    * something like [[ValveFlowBenchmark]] to stress-test the valve.
    *
    * If this becomes a bottleneck, the reference could be replaced with a
    * single atomic long (even just a single volatile long, although the atomic
    * update interface is different in JDK8 vs. JDK9+ - see `VarHandles`). The
    * long must track at least two bits of information though: one which is the
    * `closedCount` and another which is a sequence number. The purpose of the
    * sequence number is to avoid a race between `close` and `open` on an
    * initially open valve. Although just the atomic closed counter suffices
    * for `close` and `open` to both know whether or not they just transitioned
    * the valve state from close-to-open or open-to-close, it doesn't carry
    * enough information for them to know if their subsequent update to the
    * `Promise` is stale (eg. `close` incremented the closed count first, but
    * `open` decremented the closed count back down _and_ updated the promise
    * before `close` gets to looking at the promise).
    */
  private val state = new AtomicReference(new SharedValve.ValveState(0, null))

  /** If the valve is open, return [[None]]. If the valve is closed, return a
    * future which will complete when the valve is next opened.
    */
  def getOpenSignal: Option[Future[Unit]] = {
    val p = state.get.completion
    if (p eq null) None else Some(p.future)
  }

  /** Open the valve once
    */
  def open(): Unit = {
    val prevState = state.getAndUpdate { (s: SharedValve.ValveState) =>
      val newClosedCount = s.closedCount - 1
      val newCompletion = if (newClosedCount == 0) null else s.completion
      new SharedValve.ValveState(newClosedCount, newCompletion)
    }

    // This is the case where `state.completion` doesn't get copied over
    if (prevState.closedCount == 1) {
      prevState.completion.trySuccess(())
      ()
    }
  }

  /** Close the valve once
    *
    * @note this must only be called after the valve has been opened at least once!
    */
  def close(): Unit = {
    state.updateAndGet { (s: SharedValve.ValveState) =>
      val newClosedCount = s.closedCount + 1
      val newCompletion = if (newClosedCount == 1) Promise[Unit]() else s.completion
      new SharedValve.ValveState(newClosedCount, newCompletion)
    }
    ()
  }

  /** How many times over has the valve been closed */
  def getClosedCount: Int = state.get().closedCount

  override def toString: String = getClosedCount match {
    case 0 => s"SharedValve($name - open)"
    case n => s"SharedValve($name - closed[$n])"
  }

  /** @return a flow of the requested type, linked to this valve instance */
  def flow[A]: Graph[FlowShape[A, A], NotUsed] = new ValveFlow[A](this).named(s"shared-valve-$name")
}
object SharedValve {

  /** Atomic state of a shared valve
    *
    * Invariant: `completion` is `null` iff `closedCount` is `0`
    *
    * @param closedCount times valve been closed minus times its has been opened
    * @param completion promise tracking when the valve will next be open
    */
  final private class ValveState(val closedCount: Int, val completion: Promise[Unit])
}

/** Flow which automatically starts/stops based on a shared valve state.
  *
  * @param valve shared valve that dictates whether the flow lets items through or not
  */
class ValveFlow[A](valve: SharedValve) extends GraphStage[FlowShape[A, A]] {

  override val shape: FlowShape[A, A] = FlowShape(Inlet[A]("valveflow.in"), Outlet[A]("valveflow.out"))

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      import shape._

      setHandlers(in, out, this)

      var pushCallback: AsyncCallback[Unit] = _
      var willStop: Boolean = false
      var currentElement: A = null.asInstanceOf[A]

      override def preStart(): Unit =
        pushCallback = getAsyncCallback[Unit] { _ =>
          push(out, currentElement)
          currentElement = null.asInstanceOf[A]
          if (willStop) completeStage()
        }

      override def onUpstreamFinish(): Unit =
        if (isAvailable(out) && currentElement != null) willStop = true
        else completeStage()

      override def onPush(): Unit = {
        val elem = grab(in)

        valve.getOpenSignal match {
          case None =>
            push(out, elem)
          case Some(fut) =>
            currentElement = elem
            fut.onComplete(_ => pushCallback.invoke(()))(materializer.executionContext)
        }
      }

      override def onPull(): Unit = pull(in)
    }
}
