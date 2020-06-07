package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HighLowTempOptimize {

  case class MinMaxTemp(id: String,
                        min: Double,
                        max: Double,
                        endTs: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env.addSource(new SensorSource)
    readings
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new AggTemp, new WindowTemp)
      .print()

    env.execute()
  }

  class WindowTemp extends ProcessWindowFunction[(String, Double, Double),
    MinMaxTemp, String, TimeWindow] {
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Double, Double)],
                         out: Collector[MinMaxTemp]): Unit = {
      val agg = elements.head
      out.collect(MinMaxTemp(key, agg._2, agg._3, context.window.getEnd))
    }
  }

  class AggTemp extends AggregateFunction[SensorReading,
    (String, Double, Double), (String, Double, Double)] {
    override def createAccumulator(): (String, Double, Double) = {
      ("", Double.MaxValue, Double.MinValue)
    }
    override def add(value: SensorReading, accumulator: (String, Double, Double)): (String, Double, Double) = {
      (value.id, accumulator._2.min(value.temperature), accumulator._3.max(value.temperature))
    }
    override def getResult(accumulator: (String, Double, Double)): (String, Double, Double) = {
      accumulator
    }
    override def merge(a: (String, Double, Double), b: (String, Double, Double)): (String, Double, Double) = {
      (a._1, a._2.min(b._2), a._3.max(b._3))
    }
  }
}