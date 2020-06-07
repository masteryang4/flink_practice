package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._

// DataStream => KeyedStream => DataStream
object KeybyExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream : DataStream[SensorReading] = env
      .addSource(new SensorSource)

    val keyed : KeyedStream[SensorReading, String] = stream.keyBy(_.id)

    val min : DataStream[SensorReading] = keyed.min("temperature")

    min.print()

    env.execute()
  }
}