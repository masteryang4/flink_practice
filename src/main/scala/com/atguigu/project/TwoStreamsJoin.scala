package com.atguigu.project

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TwoStreamsJoin {

  // 订单事件
  case class OrderEvent(orderId: String,
                        eventType: String,
                        eventTime: String)

  // 支付事件
  case class PayEvent(orderId: String,
                      eventType: String,
                      eventTime: String)

  // 未被匹配的订单事件
  val unmatchedOrders = new OutputTag[String]("unmatched-orders"){}

  // 未被匹配的支付事件
  val unmatchedPays = new OutputTag[String]("unmatched-pays"){}


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 服务器端数据库的支付信息
    val orders = env
      .fromElements(
        OrderEvent("1", "pay", "4"),
        OrderEvent("2", "pay", "5"),
        OrderEvent("3", "pay", "9")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.orderId)

    // 远程的支付信息
    val pays = env
      .fromElements(
        PayEvent("1", "weixin", "7"),
        PayEvent("2", "zhifubao", "8"),
        PayEvent("4", "zhifubao", "10")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.orderId)

    val processed = orders
      .connect(pays)
      .process(new RealTimeCheck)

    processed.print() // 对账成功的
    processed.getSideOutput(unmatchedPays).print()
    processed.getSideOutput(unmatchedOrders).print()

    env.execute()
  }

  class RealTimeCheck extends CoProcessFunction[OrderEvent, PayEvent, String] {
    // 用来保存订单事件
    lazy val orderState = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("order-state", Types.of[OrderEvent])
    )
    // 用来保存支付事件
    lazy val payState = getRuntimeContext.getState(
      new ValueStateDescriptor[PayEvent]("pay-state", Types.of[PayEvent])
    )

    // 每来一条订单事件，调用一次
    override def processElement1(value: OrderEvent,
                                 ctx: CoProcessFunction[OrderEvent, PayEvent, String]#Context,
                                 out: Collector[String]): Unit = {
      val pay = payState.value()

      if (pay != null) { // 说明相同order-id的pay事件已经到了
        payState.clear()
        // value.orderId == pay.orderId
        out.collect("订单ID为：" + value.orderId + "的实时对账成功了！")
      } else { // 如果payState中为空，说明相同order-id的pay事件还没来
        orderState.update(value) // 保存当前来的订单，等待pay事件
        ctx.timerService().registerEventTimeTimer(value.eventTime.toLong * 1000 + 5000L)
      }
    }

    // 每来一条支付事件，调用一次
    override def processElement2(value: PayEvent,
                                 ctx: CoProcessFunction[OrderEvent, PayEvent, String]#Context,
                                 out: Collector[String]): Unit = {
      val order = orderState.value()

      if (order != null) {
        orderState.clear()
        out.collect("订单ID为：" + value.orderId + "的实时对账成功了！")
      } else {
        payState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime.toLong * 1000 + 5000L)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[OrderEvent, PayEvent, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      if (payState.value != null) {
        ctx.output(unmatchedPays, "订单ID为：" + payState.value.orderId + "的实时对账失败了！")
      }
      if (orderState.value != null) {
        ctx.output(unmatchedOrders, "订单ID为：" + orderState.value.orderId + "的实时对账失败了！")
      }
      payState.clear()
      orderState.clear()
    }
  }
}