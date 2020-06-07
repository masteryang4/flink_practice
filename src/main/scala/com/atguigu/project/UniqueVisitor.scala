package com.atguigu.project

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.Set

// 一段时间有多少用户访问了网站，涉及到去重
// 滑动窗口，长度1小时，滑动距离5秒钟，每小时独立访问用户上亿
// 阿里双十一
// 这道题的来源：阿里负责双十一大屏的工程师问的
// 每个userid占用1KB空间，10 ^ 9 个userid占用多少？--> 1TB
// 海量数据去重只有一种办法：布隆过滤器
object UniqueVisitor {
  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Int,
                          behavior: String,
                          timestamp: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/Flink1125SH/src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp)
      .map(r => (r.userId, "key"))
      .keyBy(_._2)
      .timeWindow(Time.hours(1))
      .process(new ComputeUV)

    stream.print()
    env.execute()
  }

  class ComputeUV extends ProcessWindowFunction[(Long, String), String, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(Long, String)],
                         out: Collector[String]): Unit = {
      var s: Set[Long] = Set()

      for (e <- elements) {
        s += e._1
      }

      out.collect("窗口结束时间为：" + new Timestamp(context.window.getEnd) + "的窗口的UV数据是：" + s.size)
    }
  }
}