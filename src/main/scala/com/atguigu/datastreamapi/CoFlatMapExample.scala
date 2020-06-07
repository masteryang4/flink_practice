package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoFlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env
      .fromElements(
        (1, "first stream"),
        (2, "first stream")
      )

    val stream2 = env
      .fromElements(
        (1, "second stream"),
        (2, "second stream")
      )

    stream1
      .keyBy(_._1)
      .connect(stream2.keyBy(_._1))
      .flatMap(new MyFlatMap)
      .print()

    env.execute()
  }

  class MyFlatMap extends CoFlatMapFunction[(Int, String), (Int, String), String] {
    override def flatMap1(in1: (Int, String), collector: Collector[String]): Unit = {
      collector.collect(in1._2)
    }
    override def flatMap2(in2: (Int, String), collector: Collector[String]): Unit = {
      collector.collect(in2._2)
      collector.collect(in2._2)
    }
  }
}