package com.atguigu.project

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

object SQLTopNItems {

  // 用户行为数据
  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Int,
                          behavior: String,
                          timestamp: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/Flink1125SH/src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp) // 数据集提前按照时间戳排了一下序

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner() // top n 需求只有blink planner支持
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.createTemporaryView("t", stream, 'itemId, 'timestamp.rowtime as 'ts)

    tEnv
        .sqlQuery(
          """
            |SELECT icount, windowEnd, row_num
            |FROM (
            |      SELECT icount,
            |             windowEnd,
            |             ROW_NUMBER() OVER (PARTITION BY windowEnd ORDER BY icount DESC) as row_num
            |      FROM
            |           (SELECT count(itemId) as icount,
            |                   HOP_END(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as windowEnd
            |            FROM t GROUP BY HOP(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR), itemId) as topn
            |)
            |WHERE row_num <= 3
            |""".stripMargin)
        // (itemId, ts, rank)
        .toRetractStream[(Long, Timestamp, Long)]
        .print()

    env.execute()
  }

}