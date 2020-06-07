package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._

object ReduceExample1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream: DataStream[(String, List[String])] = env.fromElements(
      ("en", List("tea")), ("fr", List("vin")), ("en", List("cake")))

    val resultStream: DataStream[(String, List[String])] = inputStream
      .keyBy(0)
      .reduce((x, y) => (x._1, x._2 ::: y._2))

    resultStream.print()

    env.execute()
  }
}