package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._

object ReduceExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .addSource(new SensorSource)

    val keyed = stream.keyBy(r => r.id)

    val min = keyed
      .reduce((x, y) => SensorReading(x.id, x.timestamp, x.temperature.min(y.temperature)))

    min.print()

    env.execute()
  }
}