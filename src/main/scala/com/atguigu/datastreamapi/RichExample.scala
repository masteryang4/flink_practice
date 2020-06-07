package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object RichExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .fromElements(1,2,3)

    stream
      .map(
        new RichMapFunction[Int, Int] {
          override def open(parameters: Configuration): Unit = {
            println("开始map的生命周期")
          }

          override def map(value: Int): Int = value + 1

          override def close(): Unit = {
            println("结束map的生命周期")
          }
        }
      )
      .print()

    env.execute()
  }
}