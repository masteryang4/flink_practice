package com.atguigu.datastreamapi

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

object FlinkTableExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
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

    val stream = env.addSource(new SensorSource)

    val table = tableEnv.fromDataStream(stream)

    table
      .select('id, 'temperature)
      .toAppendStream[(String, Double)]
      .print()

    env.execute()
  }
}