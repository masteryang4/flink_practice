package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._

object UnionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val parisStream: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .filter(r => r.id == "sensor_1")
    val tokyoStream: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .filter(r => r.id == "sensor_2")
    val rioStream: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .filter(r => r.id == "sensor_3")
    val allCities: DataStream[SensorReading] = parisStream
      .union(tokyoStream, rioStream)

    allCities.print()

    env.execute()
  }
}