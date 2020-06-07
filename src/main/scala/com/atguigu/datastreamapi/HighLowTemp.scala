package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HighLowTemp {

  case class MinMaxTemp(id: String,
                        min: Double,
                        max: Double,
                        endTs: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env.addSource(new SensorSource)

    readings
      .keyBy(r => r.id)
      .timeWindow(Time.seconds(5))
      .process(new HighLow)
      .print()

    env.execute()
  }

  class HighLow extends ProcessWindowFunction[SensorReading,
    MinMaxTemp, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[SensorReading],
                         out: Collector[MinMaxTemp]): Unit = {
      val temps = elements.map(r => r.temperature)
      val endTs = context.window.getEnd
      out.collect(MinMaxTemp(key, temps.min, temps.max, endTs))
    }
  }
}