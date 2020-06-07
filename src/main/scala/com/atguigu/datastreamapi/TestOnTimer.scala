package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TestOnTimer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .process(new Key)

    stream.print()
    env.execute()
  }
  class Key extends KeyedProcessFunction[String, (String, Long), String] {
    override def processElement(value: (String, Long),
                                ctx: KeyedProcessFunction[String, (String, Long), String]#Context,
                                out: Collector[String]): Unit = {
      ctx.timerService().registerEventTimeTimer(value._2 + 10 * 1000L - 1)
    }
    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      println("定时事件发生的时间：" + timestamp)
      out.collect("定时事件发生了！！！")
    }
  }
}