package com.atguigu.datastreamapi

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

class SensorSource extends RichParallelSourceFunction[SensorReading] {

  var running = true

  override def run(sourceContext: SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    var curFTemp = (1 to 10).map(
      i => ("sensor_" + i, 65 + (rand.nextGaussian() * 20))
    )

    while (running) {

      curFTemp = curFTemp.map(
        t => (t._1, t._2 + (rand.nextGaussian() * 0.5))
      )

      val curTime = Calendar.getInstance.getTimeInMillis

      curFTemp.foreach(t => sourceContext.collect(
        SensorReading(t._1, curTime, t._2)
      ))

      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = running = false
}