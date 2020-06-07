package com.atguigu.datastreamapi

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Tumble}
import org.apache.flink.table.api.scala._

object FlinkTableExample1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(
      env,
      settings
    )

    val stream = env
      .addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)

    val table = tableEnv
      // 将DataStream转换成动态表
      .fromDataStream(stream, 'id, 'ts.rowtime, 'temperature)
      // 10s的滚动窗口
      .window(Tumble over 10.seconds on 'ts as 'tw)
      // .keyBy(_.id).timeWindow(Time.seconds(10).process)
      .groupBy('id, 'tw)
      // 将动态表转换成了另一张动态表
      .select('id, 'id.count) // 无法做增量聚合

    println(tableEnv.explain(table))

    table
      // 将动态表转换成了DataStream
        .toRetractStream[(String, Long)]
        .print()


    env.execute()
  }
}