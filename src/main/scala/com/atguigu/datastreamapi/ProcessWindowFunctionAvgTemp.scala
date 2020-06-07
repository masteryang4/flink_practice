package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object ProcessWindowFunctionAvgTemp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .process(new MyAvgTemp)

    readings.print()
    env.execute()
  }

  class MyAvgTemp extends ProcessWindowFunction[
    (String, Double), (String, Double, Long), String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Double)],
                         out: Collector[(String, Double, Long)]): Unit = {
      var sum = 0.0
      val size = elements.size
      for (i <- elements) {
        sum += i._2
      }
      out.collect((key, sum / size, context.window.getEnd))
    }
  }
}