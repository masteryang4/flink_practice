package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._

object SplitExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
    println(env.getParallelism)
    val inputStream: DataStream[(Int, String)] = env
      .fromElements(
        (1001, "1001"),
        (999, "999")
      ).setParallelism(1)

    val s = env
      .fromElements(
        Tuple1(1)
      )

    val splitted: SplitStream[(Int, String)] = inputStream
      .split(t => if (t._1 > 1000) Seq("large") else Seq("small"))

    val large: DataStream[(Int, String)] = splitted.select("large")
    val small: DataStream[(Int, String)] = splitted.select("small")
    val all: DataStream[(Int, String)] = splitted.select("small", "large")

    large.print()

    env.execute()
  }
}