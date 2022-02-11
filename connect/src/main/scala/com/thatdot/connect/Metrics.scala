package com.thatdot.connect

import java.lang.management.ManagementFactory

import scala.collection.mutable.ListBuffer

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jvm.{BufferPoolMetricSet, GarbageCollectorMetricSet, MemoryUsageGaugeSet}

import com.thatdot.connect.config.{MetricsReporter, ReporterWrapper}

object Metrics extends MetricRegistry {

  val garbageCollection: GarbageCollectorMetricSet = register("gc", new GarbageCollectorMetricSet())
  val memoryUsage: MemoryUsageGaugeSet = register("memory", new MemoryUsageGaugeSet())
  val bufferPools: BufferPoolMetricSet =
    register("buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer))

  private val reporters: ListBuffer[ReporterWrapper] = ListBuffer.empty[ReporterWrapper]
  def addReporter(reporter: MetricsReporter, namespace: String): Unit = {
    reporters += reporter.register(this, namespace)
    ()
  }

  def startReporters(): Unit = reporters.foreach(_.start())
  def stopReporters(): Unit = reporters.foreach(_.stop())
}
