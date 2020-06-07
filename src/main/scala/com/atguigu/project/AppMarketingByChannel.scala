package com.atguigu.project

import java.sql.Timestamp
import java.util.{Calendar, UUID}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

object AppMarketingByChannel {

  case class MarketingUserBehavior(userId: String,
                                   behavior: String,
                                   channel: String,
                                   ts: Long)

  class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehavior] {
    var running = true

    // fake渠道信息
    val channelSet = Seq("AppStore", "XiaomiStore")
    // fake用户行为信息
    val behaviorTypes = Seq("BROWSE", "CLICK", "UNINSTALL", "INSTALL")

    val rand = new Random

    override def run(ctx: SourceContext[MarketingUserBehavior]): Unit = {
      while (running) {
        // UUID产生一个唯一的字符串，本质就是哈希
        val userId = UUID.randomUUID().toString
        val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
        val channel = channelSet(rand.nextInt(channelSet.size))
        val ts = Calendar.getInstance().getTimeInMillis

        ctx.collect(MarketingUserBehavior(userId, behaviorType, channel, ts))

        Thread.sleep(10)
      }
    }

    override def cancel(): Unit = running = false

  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.ts)
      .filter(_.behavior != "UNINSTALL")
      .map(r => {
        ((r.channel, r.behavior), 1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .process(new MarketingCountByChannel)

    stream.print()
    env.execute()
  }

  class MarketingCountByChannel extends ProcessWindowFunction[((String, String), Long), (String, Long, Timestamp), (String, String), TimeWindow] {
    override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[(String, Long, Timestamp)]): Unit = {
      out.collect((key._1, elements.size, new Timestamp(context.window.getEnd)))
    }
  }
}