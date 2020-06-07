package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TestOnTimerProcessingTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .process(new Key)

    stream.print()
    env.execute()
  }
  class Key extends KeyedProcessFunction[String, SensorReading, SensorReading] {
    override def processElement(value: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#Context,
                                out: Collector[SensorReading]): Unit = {
      out.collect(value)
      ctx.timerService().registerProcessingTimeTimer(value.timestamp + 10 * 1000L)
    }
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {
      println("定时事件执行了！！！")
    }
  }
}