package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.{AggregateFunction, TableAggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object UDFAggregateFunctionExample {
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

    val avgTemp = new AvgTemp

//    table
//        .groupBy('id)
//        .aggregate(avgTemp('temperature) as 'avgTemp)
//        .select('id, 'avgTemp)
//        .toRetractStream[(String, Double)]
//        .print()

    // sql实现
    tEnv.createTemporaryView("sensor", table)
    tEnv.registerFunction("avgTemp", avgTemp)

    tEnv
        .sqlQuery(
          """
            |SELECT
            |id, avgTemp(temperature)
            |FROM sensor
            |GROUP BY id
            |""".stripMargin
        )
        .toRetractStream[(String, Double)]
        .print()

    env.execute()
  }

  class AvgTempAcc {
    var sum: Double = 0.0
    var count: Int = 0
  }

  class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {
    override def createAccumulator(): AvgTempAcc = new AvgTempAcc

    override def getValue(accumulator: AvgTempAcc): Double = {
      accumulator.sum / accumulator.count
    }

    def accumulate(acc: AvgTempAcc, temp: Double): Unit = {
      acc.sum += temp
      acc.count += 1
    }
  }

}