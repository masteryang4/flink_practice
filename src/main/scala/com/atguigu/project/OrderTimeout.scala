package com.atguigu.project

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.Map

object OrderTimeout {

  case class OrderEvent(orderId: String,
                        eventType: String,
                        eventTime: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .fromElements(
        OrderEvent("1", "create", "2"),
        OrderEvent("2", "create", "3"),
        OrderEvent("2", "pay", "4")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.orderId)

    val pattern = Pattern
      .begin[OrderEvent]("start")
      .where(_.eventType.equals("create"))
      .next("end")
      .where(_.eventType.equals("pay"))
      .within(Time.seconds(5))

    val patternStream = CEP.pattern(stream, pattern)

    // 将超时的订单发送到侧输出标签
    val orderTimeoutOutputTag = OutputTag[String]("order-timeout")

    // 处理超时事件的函数
    val timeoutFunc = (map: Map[String, Iterable[OrderEvent]], ts: Long, out: Collector[String]) => {
      val order = map("start").head
      out.collect("超时订单的ID为：" + order.orderId)
    }

    // 处理没有超时的订单
    val selectFunc = (map: Map[String, Iterable[OrderEvent]], out: Collector[String]) => {
      val order = map("end").head
      out.collect("没有超时的订单ID为：" + order.orderId)
    }

    val timeoutOrder = patternStream
      .flatSelect(orderTimeoutOutputTag)(timeoutFunc)(selectFunc)

    timeoutOrder.print()
    timeoutOrder.getSideOutput(orderTimeoutOutputTag).print()

    env.execute()
  }
}