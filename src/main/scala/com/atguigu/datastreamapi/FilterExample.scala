package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object FilterExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .addSource(new SensorSource)
//      .filter(r => r.id == "sensor_1")
        .filter(new MyFilterFunction)

    stream.print()

    env.execute()
  }

  class MyFilterFunction extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean = value.id == "sensor_1"
  }

}