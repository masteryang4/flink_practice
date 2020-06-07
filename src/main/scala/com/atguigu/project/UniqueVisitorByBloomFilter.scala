package com.atguigu.project

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import scala.collection.Set

// 一段时间有多少用户访问了网站，涉及到去重
// 滑动窗口，长度1小时，滑动距离5秒钟，每小时独立访问用户上亿
// 阿里双十一
// 这道题的来源：阿里负责双十一大屏的工程师问的
// 每个userid占用1KB空间，10 ^ 9 个userid占用多少？--> 1TB
// 海量数据去重只有一种办法：布隆过滤器
object UniqueVisitorByBloomFilter {

  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Int,
                          behavior: String,
                          timestamp: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty(
      "key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    properties.setProperty(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    properties.setProperty(
      "auto.offset.reset",
      "latest"
    )

    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/Flink1125SH/src/main/resources/UserBehavior.csv")
//      .addSource(new FlinkKafkaConsumer011[String](
//        "hotitems1",
//        new SimpleStringSchema(),
//        properties
//      ))
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp)
      .map(r => (r.userId, "key"))
      .keyBy(_._2)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger)
      .process(new FilterWindow)

    env.execute()
  }

  // 触发器的作用：每来一条数据，就触发窗口的计算（去重的逻辑），并清空窗口
  // 类似于增量聚合
  class MyTrigger extends Trigger[(Long, String), TimeWindow] {
    override def onElement(element: (Long, String),
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: TriggerContext): TriggerResult = {
      // 触发窗口计算，清空窗口
      TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(time: Long,
                                  window: TimeWindow,
                                  ctx: TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long,
                             window: TimeWindow,
                             ctx: TriggerContext): TriggerResult = {
      if (ctx.getCurrentWatermark >= window.getEnd) {
        val jedis = new Jedis("localhost", 6379)
        val key = window.getEnd.toString
        println(new Timestamp(key.toLong), jedis.hget("UvCountHashTable", key))
        TriggerResult.FIRE_AND_PURGE
      }
      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: TriggerContext): Unit = {}
  }

  class FilterWindow extends ProcessWindowFunction[(Long, String), String, String, TimeWindow] {
    lazy val jedis = new Jedis("localhost", 6379)

    override def process(key: String,
                         context: Context,
                         elements: Iterable[(Long, String)],
                         out: Collector[String]): Unit = {
      // redis保存一张哈希表
      // `UvCountHashTable`: `windowEnd` -> `count`
      // 还需要维护很多键值对
      // `windowEnd` -> `bit array`

      var count = 0L

      val key = context.window.getEnd.toString // windowEnd

      // 如果窗口的uv值不为0，取出计数值
      if (jedis.hget("UvCountHashTable", key) != null) {
        count = jedis.hget("UvCountHashTable", key).toLong
      }

      val userId = elements.head._1.toString // 取出userid
      val offset = bloomHash(userId, 1 << 8) // userId经过hash以后在位数组中的下标

      val isExist = jedis.getbit(key, offset) // getbit会自动创建key对应的位数组，如果位数组不存在的话
      if (!isExist) {
        // 如果offset下标对应的比特数组的相应位为0，那么翻转为1
        jedis.setbit(key, offset, true)
        // 由于userid不存在，所以uv数量加一
        jedis.hset("UvCountHashTable", key, (count + 1).toString)
      }
    }

    def bloomHash(userId: String, bitArraySize: Long): Long = {
      var result = 0
      for (i <- 0 until userId.length) {
        result = result * 61 + userId.charAt(i)
      }
      (bitArraySize - 1) & result
    }
  }
}