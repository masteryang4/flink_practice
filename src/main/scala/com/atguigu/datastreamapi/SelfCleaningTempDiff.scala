package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SelfCleaningTempDiff {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .process(new SelfCleaningTemperatureAlertFunction(1.7))

    readings.print()
    env.execute()
  }

  class SelfCleaningTemperatureAlertFunction(val threshold: Double)
    extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {
    lazy val lastTemp = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("last-temp", Types.of[Double])
    )
    lazy val lastTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("last-timer", Types.of[Long])
    )

    override def processElement(value: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
      val newTimer = value.timestamp + (3600 * 1000)
      val curTimer = lastTimer.value()
      ctx.timerService().deleteProcessingTimeTimer(curTimer)
      ctx.timerService().registerProcessingTimeTimer(newTimer)
      lastTimer.update(newTimer)

      val last = lastTemp.value()
      val tempDiff = (value.temperature - last).abs
      if (tempDiff > threshold) {
        out.collect((value.id, value.temperature, tempDiff))
      }
      lastTemp.update(value.temperature)
    }

    override def onTimer(ts: Long,
                         ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#OnTimerContext,
                         out: Collector[(String, Double, Double)]): Unit = {
      lastTemp.clear()
      lastTimer.clear()
    }
  }



}