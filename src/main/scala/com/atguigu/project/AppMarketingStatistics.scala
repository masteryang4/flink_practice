package com.atguigu.project

import java.sql.Timestamp

import com.atguigu.project.AppMarketingByChannel.{MarketingUserBehavior, SimulatedEventSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AppMarketingStatistics {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.ts)
      .filter(_.behavior != "UNINSTALL")
      .timeWindowAll(Time.seconds(5), Time.seconds(1))
      .process(new MarketingCountTotal)
    stream.print()
    env.execute()
  }

  class MarketingCountTotal extends ProcessAllWindowFunction[MarketingUserBehavior, (Long, Timestamp), TimeWindow] {
    override def process(context: Context, elements: Iterable[MarketingUserBehavior], out: Collector[(Long, Timestamp)]): Unit = {
      out.collect((elements.size, new Timestamp(context.window.getEnd)))
    }
  }
}