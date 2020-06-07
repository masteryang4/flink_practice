package com.atguigu.project

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object TwoStreamsIntervalJoin {
  // 订单事件
  case class OrderEvent(orderId: String,
                        eventType: String,
                        eventTime: String)

  // 支付事件
  case class PayEvent(orderId: String,
                      eventType: String,
                      eventTime: String)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 服务器端数据库的支付信息
    val orders = env
      .fromElements(
        OrderEvent("1", "pay", "4"),
        OrderEvent("2", "pay", "5"),
        OrderEvent("3", "pay", "9"),
        OrderEvent("5", "pay", "10")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.orderId)

    // 远程的支付信息
    val pays = env
      .fromElements(
        PayEvent("1", "weixin", "7"),
        PayEvent("2", "zhifubao", "8"),
        PayEvent("4", "zhifubao", "10"),
        PayEvent("5", "zhifubao", "20")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.orderId)

    orders
      .intervalJoin(pays)
      .between(Time.seconds(-5),Time.seconds(5))
      .process(new MyIntervalJoin)
      .print()

    env.execute()
  }

  class MyIntervalJoin extends ProcessJoinFunction[OrderEvent, PayEvent, String] {
    override def processElement(left: OrderEvent,
                                right: PayEvent,
                                context: ProcessJoinFunction[OrderEvent, PayEvent, String]#Context,
                                out: Collector[String]
                               ): Unit = {
      out.collect(left +" =Interval Join=> "+right)
    }
  }
}