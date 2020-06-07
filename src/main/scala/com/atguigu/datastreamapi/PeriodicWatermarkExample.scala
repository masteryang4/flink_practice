package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object PeriodicWatermarkExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)

    val stream1 = env
      .addSource(new SensorSource)
      .setParallelism(1)
//      .assignTimestampsAndWatermarks(new MyAssigner)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .process(new MyProcess)

    val stream2 = env
        .addSource(new SensorSource)
        .setParallelism(1)
        .assignAscendingTimestamps(_.timestamp)

    stream1.print()
    env.execute()
  }

  class MyProcess extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[SensorReading],
                         out: Collector[String]): Unit = {
      out.collect("hello world")
    }
  }

  class MyAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
    val bound: Long = 0L // 最大延迟时间
    var maxTs: Long = Long.MinValue + bound // 用来保存观察到的最大事件时间

    // 每来一条数据，调用一次
    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
      maxTs = maxTs.max(element.timestamp) // 更新观察到的最大事件时间
      element.timestamp // 提取事件时间戳
    }

    // 系统插入水位线的时候调用
    override def getCurrentWatermark: Watermark = {
      // 水位线 = 观察到的最大事件时间 - 最大延迟时间
      new Watermark(maxTs - bound)
    }
  }
}