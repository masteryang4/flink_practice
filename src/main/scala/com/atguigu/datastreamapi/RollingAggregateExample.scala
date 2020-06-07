package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._

object RollingAggregateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[(Int, Int, Int)] = env.fromElements(
      (1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3))

    val resultStream: DataStream[(Int, Int, Int)] = inputStream
      .keyBy(0) // key on first field of the tuple
      .sum(1)   // sum the second field of the tuple in place

    resultStream.print()

    env.execute()
  }
}