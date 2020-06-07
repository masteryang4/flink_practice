package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object ConnectExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val one: DataStream[(Int, Long)] = env
      .fromElements(
        (1, 1L),
        (2, 2L),
        (3, 3L)
      )
    val two: DataStream[(Int, String)] = env
      .fromElements(
        (1, "1"),
        (2, "2"),
        (3, "3")
      )

    val connected : ConnectedStreams[(Int, Long), (Int, String)] = one
      .keyBy(_._1)
      .connect(
        two
          .keyBy(_._1)
      )

    val comap = connected
      .map(new MyCoMap)

    comap.print()

    env.execute()
  }

  class MyCoMap extends CoMapFunction[(Int, Long),
    (Int, String), String] {
    override def map1(in1: (Int, Long)): String = {
      "key为 " + in1._1 + " 的数据来自第一条流"
    }

    override def map2(in2: (Int, String)): String = {
      "key为 " + in2._1 + " 的数据来自第二条流"
    }
  }
}