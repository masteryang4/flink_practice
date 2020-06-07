package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .process(new FreezingMonitor)

    readings
        .getSideOutput(new OutputTag[String]("freezing-alarm"))
        .print()

//    readings.print()

    env.execute()
  }

  // 没有keyby，所以使用ProcessFunction
  class FreezingMonitor extends KeyedProcessFunction[String, SensorReading, SensorReading] {
    lazy val freezingAlarmOutput = new OutputTag[String]("freezing-alarm")

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature < 32.0) {
        ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${value.id}")
      }
      out.collect(value)
    }
  }
}