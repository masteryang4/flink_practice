package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

object EveryTenSec {
  def main(args: Array[String]): Unit = {
    // 初始化执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 初始化表环境
    val settings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment
      .create(env, settings)

    // 流数据
    val stream = env
      .addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)

    // 从流创建表
    val dataTable = tableEnv
      .fromDataStream(stream, 'id, 'ts.rowtime, 'temperature)

    // 写sql完成开窗统计
    tableEnv
      .sqlQuery(
        "select id, count(id) from " + dataTable +
          " group by id, tumble(ts, interval '10' second)"
      )
      // 只要使用了group by, 必须使用撤回流
      .toRetractStream[(String, Long)]
      .print()

    env.execute()
  }
}