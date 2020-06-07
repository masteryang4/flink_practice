package com.atguigu.project

import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object LoginFailWithoutCEP {

  case class LoginEvent(userId: String,
                        ipAddr: String,
                        eventType: String,
                        eventTime: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .fromElements(
        LoginEvent("1", "0.0.0.0", "fail", "1"),
        LoginEvent("1", "0.0.0.0", "success", "2"),
        LoginEvent("1", "0.0.0.0", "fail", "3"),
        LoginEvent("1", "0.0.0.0", "fail", "4")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy(_.userId)
      .process(new MatchFunction)

    stream.print()
    env.execute()
  }

  class MatchFunction extends KeyedProcessFunction[String, LoginEvent, String] {
    lazy val loginState = getRuntimeContext.getListState(
      new ListStateDescriptor[LoginEvent]("login-state", Types.of[LoginEvent])
    )
    lazy val tsState = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("ts-state", Types.of[Long])
    )
    override def processElement(value: LoginEvent,
                                ctx: KeyedProcessFunction[String, LoginEvent, String]#Context,
                                out: Collector[String]): Unit = {
      if (value.eventType.equals("fail")) {
        loginState.add(value) // 用来保存登录失败的事件
        if (tsState.value() == 0L) { // 检查一下有没有定时器存在
          // 5s中之内连续2次登录失败，报警
          ctx.timerService().registerEventTimeTimer(value.eventTime.toLong * 1000 + 5000L)
          tsState.update(value.eventTime.toLong * 1000 + 5000L)
        }
      }

      if (value.eventType.equals("success")) {
        loginState.clear()
        ctx.timerService().deleteEventTimeTimer(tsState.value())
        tsState.clear()
      }
    }
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, LoginEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allLogins = ListBuffer[LoginEvent]()
      import scala.collection.JavaConversions._
      for (login <- loginState.get) {
        allLogins += login
      }
      loginState.clear()
      if (allLogins.length >= 2) {
        out.collect("5s之内连续两次登录失败")
      }
    }
  }
}