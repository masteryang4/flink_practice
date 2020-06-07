package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream : DataStream[Long] = env
//      .addSource(new SensorSource)
        .fromElements(
          1L, 2L, 3L
        )
        .flatMap(new MyFlatMapFunction)

    stream.print()

    env.execute()
  }

  class MyFlatMapFunction extends FlatMapFunction[Long, Long] {
    override def flatMap(value: Long, out: Collector[Long]): Unit = {
      if (value == 2) {
        out.collect(value)
      } else if (value == 3) {
        out.collect(value)
        out.collect(value)
      }
    }
  }
}