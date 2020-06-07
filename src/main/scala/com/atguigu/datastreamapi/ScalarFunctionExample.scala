package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction

object ScalarFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val stream = env.addSource(new SensorSource)

    // 初始化udf函数
    val hashCode = new HashCode(10)
    tEnv
      // 注册udf函数
      .registerFunction("hashCode", hashCode)

    val table = tEnv.fromDataStream(stream, 'id)

    // 在table api中使用udf函数
    table
      .select('id, hashCode('id))
      .toAppendStream[(String, Int)]
      .print()

    // 在flink sql中使用udf函数

    // 创建视图
    tEnv.createTemporaryView("t", table, 'id)

    tEnv
        .sqlQuery("SELECT id, hashCode(id) FROM t")
        .toAppendStream[(String, Int)]
        .print()

    env.execute()

  }

  class HashCode(factor: Int) extends ScalarFunction {
    def eval(s: String): Int = {
      s.hashCode * factor
    }
  }
}