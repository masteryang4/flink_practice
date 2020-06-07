package com.atguigu.datastreamapi

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TempIncreaseAlert {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .process(new TempIncreaseAlertFunction)

    readings.print()
    env.execute()
  }

  class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {
    var lastTemp : ValueState[Double] = _
    var currentTimer : ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
      lastTemp = getRuntimeContext.getState(
        new ValueStateDescriptor[Double]("last-temp", Types.of[Double])
      )
      currentTimer = getRuntimeContext.getState(
        new ValueStateDescriptor[Long]("timer", Types.of[Long])
      )
    }

    override def processElement(r: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                                out: Collector[String]): Unit = {
      val prevTemp = lastTemp.value()
      lastTemp.update(r.temperature)

      val curTimerTimestamp = currentTimer.value()

      if (prevTemp == 0.0 || r.temperature < prevTemp) {
        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
        currentTimer.clear()
      } else if (r.temperature > prevTemp && curTimerTimestamp == 0L) {
        val timerTs = ctx.timerService().currentProcessingTime() + 1000L
        ctx.timerService().registerProcessingTimeTimer(timerTs)
        currentTimer.update(timerTs)
      }
    }
    override def onTimer(ts: Long,
                         ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      out.collect("温度连续1s上升了！" + ctx.getCurrentKey)
    }
  }
}