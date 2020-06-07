package com.atguigu

import org.apache.flink.streaming.api.scala._

object StreamingJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(
      "hello world",
      "hello atguigu"
    )

    stream
        .flatMap(w => w.split(" "))
        .map(r => (r, 1))
        .keyBy(0)
        .sum(1)
        .print() //  stdout

    env.execute("Flink Streaming Scala API Skeleton")
  }
}
