package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object LateSideOutput1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val readings = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      .assignAscendingTimestamps(_._2)

    val countPer10Secs = readings
      .keyBy(_._1)
      .process(new CountFunction())

    countPer10Secs
      .getSideOutput(new OutputTag[(String, Long)]("late"))
        .print()

    env.execute()
  }

  class CountFunction extends KeyedProcessFunction[String, (String, Long), (String, Long)] {
    val lateReadingOut = new OutputTag[(String, Long)]("late")

    override def processElement(value: (String, Long),
                                ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#Context,
                                out: _root_.org.apache.flink.util.Collector[(String, Long)]): Unit = {
      if (value._2 < ctx.timerService().currentWatermark()) {
        ctx.output(lateReadingOut, value)
      } else {
        out.collect(value)
      }
    }
  }
}