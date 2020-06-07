package com.atguigu

import org.apache.flink.streaming.api.scala._ // 导入所有的隐式类型转换
import org.apache.flink.streaming.api.windowing.time.Time

object WordCount {

  case class WordWithCount(word: String,
                           count: Int)

  def main(args: Array[String]): Unit = {
    // 获取运行时环境
    // 相当于sparkContext
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度为1
    env.setParallelism(1024)

    // 定义DAG
    // 数据流的来源（socket）=> 处理逻辑 => 数据流计算结果的去向，sink（print，打印到屏幕）
    val text = env
//      .fromElements(
//        "hello world",
//        "hello atguigu"
//      )
      .socketTextStream("localhost", 9999, '\n')

    val windowCount = text
      // 使用空格对字符串进行切分
      .flatMap(w => w.split("\\s"))
      // map操作
      .map(w => WordWithCount(w, 1))
      // 分组，shuffle操作
      .keyBy("word")
      // 开滚动窗口
      .timeWindow(Time.seconds(5))
      // reduce操作
      .sum("count")
    // 定义DAG结束

    // output操作，输出到标准输出，stdout
    windowCount
      .print()

    // 执行DAG
    env.execute()

  }
}