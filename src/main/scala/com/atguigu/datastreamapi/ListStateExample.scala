package com.atguigu.datastreamapi

import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object ListStateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .process(new MyKey)

    readings.print()
    env.execute()
  }

  class MyKey extends KeyedProcessFunction[String, SensorReading, String] {
    lazy val listState = getRuntimeContext.getListState(
      new ListStateDescriptor[SensorReading]("list-state", Types.of[SensorReading])
    )

    lazy val timerTs = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

    override def processElement(value: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                                out: Collector[String]): Unit = {
      listState.add(value)
      if (timerTs.value() == 0L) {
        ctx.timerService().registerProcessingTimeTimer(value.timestamp + 10 * 1000L)
        timerTs.update(value.timestamp + 10 * 1000L)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      val buffer: ListBuffer[SensorReading] = ListBuffer()
      import scala.collection.JavaConversions._
      for (reading <- listState.get) {
        buffer += reading
      }
      listState.clear()
      timerTs.clear()
      out.collect("传感器ID为" + ctx.getCurrentKey +"共" + buffer.size + "条数据")
    }
  }
}