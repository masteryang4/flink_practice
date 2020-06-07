package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

object TableAggregateFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val table = tEnv.fromDataStream(stream, 'id, 'temperature)

    val top2Temp = new Top2Temp

    table
      .groupBy('id)
      .flatAggregate(top2Temp('temperature) as ('temp, 'rank))
      .select('id, 'temp, 'rank)
      .toRetractStream[(String, Double, Int)]
      .print("agg temp")

    env.execute()
  }

  class Top2TempAcc {
    var highestTemp: Double = Int.MinValue
    var secondHighestTemp: Double = Int.MinValue
  }

  class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
    override def createAccumulator(): Top2TempAcc = new Top2TempAcc

    def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
      if (temp > acc.highestTemp) {
        acc.secondHighestTemp = acc.highestTemp
        acc.highestTemp = temp
      } else if (temp > acc.secondHighestTemp) {
        acc.secondHighestTemp = temp
      }
    }

    // 输出(温度，排名)信息
    def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
      out.collect(acc.highestTemp, 1)
      out.collect(acc.secondHighestTemp, 2)
    }
  }
}