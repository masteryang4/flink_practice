package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object EventTimeExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(60 * 1000)
    env.setParallelism(1)

    val stream = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000)
      })
      // 抽取时间戳（单位必须是ms），插入水位线
      // 水位线 = 观察到的最大事件时间 - 最大延迟时间
      .assignTimestampsAndWatermarks(
        // 设置最大延迟时间是5s
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          override def extractTimestamp(t: (String, Long)): Long = t._2
        }
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .process(new EvenTimeProcess)

    stream.print()
    env.execute()
  }

  class EvenTimeProcess extends ProcessWindowFunction[(String, Long),
    String, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[String]): Unit = {
      out.collect("窗口结束时间为：" + context.window.getEnd + " 窗口共有 " + elements.size + " 条数据")
    }
  }
}