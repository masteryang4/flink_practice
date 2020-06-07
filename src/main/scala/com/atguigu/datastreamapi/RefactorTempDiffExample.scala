package com.atguigu.datastreamapi

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RefactorTempDiffExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(10000L)
    env.setStateBackend(new FsStateBackend("file:///Users/yuanzuo/Desktop/Flink1125SH/checkpoints"))

    val readings = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double] {
        case (in: SensorReading, None) => {
          (List.empty, Some(in.temperature))
        }
        case (r: SensorReading, lastTemp: Some[Double]) => {
          val tempDiff = (r.temperature - lastTemp.get).abs
          if (tempDiff > 1.7) {
            (List((r.id, r.temperature, tempDiff)), Some(r.temperature))
          } else {
            (List.empty, Some(r.temperature))
          }
        }
      }

    readings.print()
    env.execute()
  }
}