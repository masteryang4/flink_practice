package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TempDiffExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .flatMap(new TemperatureAlertFunction(1.7))

    readings.print()
    env.execute()
  }

  class TemperatureAlertFunction(val threshold: Double)
    extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
    lazy val lastTempState = getRuntimeContext
      .getState(
        new ValueStateDescriptor[Double]("last", Types.of[Double])
      )

    override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
      val lastTemp = lastTempState.value()

      val tempDiff = (value.temperature - lastTemp).abs
      if (tempDiff > threshold) {
        out.collect((value.id, value.temperature, tempDiff))
      }
      this.lastTempState.update(value.temperature)
    }
  }
}