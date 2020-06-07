package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowMinTemp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)

    readings
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
//      .timeWindow(Time.seconds(5))
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .reduce((x1, x2) => (x1._1, x1._2.min(x2._2)))
      .print()

    env.execute()
  }
}