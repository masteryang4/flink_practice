package com.atguigu.project

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowJoinExmaple {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orangeStream = env
      .fromElements(
        (1, 1000L),
        (2, 2000L)
      )
      .assignAscendingTimestamps(_._2)

    val greenStream = env
      .fromElements(
        (1, 1200L),
        (1, 1500L),
        (2, 2000L)
      )
      .assignAscendingTimestamps(_._2)

    orangeStream
      .join(greenStream)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply { (e1, e2) => e1 + "," + e2 }
      .print()

    env.execute()
  }
}