package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object DistributedWatermark {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream1 = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)


    val stream2 = env
      .socketTextStream("localhost", 9998, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)

    stream1.connect(stream2).process(new MyCoProcess).print()
    env.execute()
  }
  class MyCoProcess extends CoProcessFunction[(String, Long), (String, Long), String] {
    override def processElement1(value: (String, Long),
                                 ctx: CoProcessFunction[(String, Long), (String, Long), String]#Context,
                                 out: Collector[String]): Unit = {
      println("from stream1", ctx.timerService().currentWatermark())
      out.collect("from stream1")
    }

    override def processElement2(value: (String, Long),
                                 ctx: CoProcessFunction[(String, Long), (String, Long), String]#Context, out: Collector[String]): Unit = {
      println("from stream2", ctx.timerService().currentWatermark())
      out.collect("from stream2")

    }
  }
}