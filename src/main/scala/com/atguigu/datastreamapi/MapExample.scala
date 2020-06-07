package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object MapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .addSource(new SensorSource)
//      .map(new MyMapFunction)
        .map(
          new MapFunction[SensorReading, String] {
            override def map(value: SensorReading): String = value.id
          }
        )

    stream.print()

    env.execute()
  }

  class MyMapFunction extends MapFunction[SensorReading, String] {
    override def map(value: SensorReading): String = value.id
  }
}