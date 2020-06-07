package com.atguigu.datastreamapi

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UpdateWindow {
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
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          override def extractTimestamp(element: (String, Long)): Long = element._2
        }
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(5))
      .process(new UpdatingWindowCountFunction)

    readings.print()
    env.execute()
  }

  class UpdatingWindowCountFunction extends ProcessWindowFunction[(String, Long),
    String, String, TimeWindow] {
    override def process(key: String,
                         context: Context, elements: Iterable[(String, Long)],
                         out: Collector[String]): Unit = {
      val cnt = elements.size

      val isUpdate = context
        .windowState
        .getState(
          new ValueStateDescriptor[Boolean]("isUpdate", Types.of[Boolean])
        )

      if (!isUpdate.value()) {
        out.collect("当水位线超过窗口结束时间时，第一次触发窗口计算，窗口中共有" +
          cnt + "条数据")
        isUpdate.update(true)
      } else {
        out.collect("迟到元素来更新窗口计算结果了！" +
          "窗口中共有" + cnt + "条数据")
      }
    }
  }
}