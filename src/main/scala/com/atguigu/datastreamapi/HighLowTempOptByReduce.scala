package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HighLowTempOptByReduce {
  case class MinMaxTemp(id: String,
                        min: Double,
                        max: Double,
                        endTs: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env.addSource(new SensorSource)
    readings
      .map(r => (r.id, r.temperature, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .reduce(
        (r1: (String, Double, Double), r2: (String, Double, Double)) => {
          (r1._1, r1._2.min(r2._2), r1._3.max(r2._3))
        },
        new WindowTemp
      )
      .print()

    env.execute()
  }

  class WindowTemp extends ProcessWindowFunction[(String, Double, Double),
    MinMaxTemp, String, TimeWindow] {
    // process函数什么时候调用？
    // 当窗口闭合的时候调用
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Double, Double)],
                         out: Collector[HighLowTempOptByReduce.MinMaxTemp]): Unit = {
      val agg = elements.head
      out.collect(MinMaxTemp(key, agg._2, agg._3, context.window.getEnd))
    }
  }
}