package com.atguigu.project

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object TopNItems {

  // 用户行为数据
  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Int,
                          behavior: String,
                          timestamp: Long)

  // 窗口聚合输出的样例类
  // 某个窗口中，某个商品的pv次数
  case class ItemViewCount(itemId: Long,
                           windowEnd: Long,
                           count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/Flink1125SH/src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp) // 数据集提前按照时间戳排了一下序
      // group by HOP(...), itemId
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg, new WindowResult) // --> 合流成DataStream
      .keyBy(_.windowEnd)
      .process(new TopN(3))

    stream.print()
    env.execute()
  }

  // 聚合的元素属于同一个Key和Window
  // 窗口闭合时，增量聚合函数将聚合结果发送给全窗口聚合函数
  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class WindowResult extends ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def process(key: Long,
                         context: Context,
                         elements: Iterable[Long],
                         out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key, context.window.getEnd, elements.head))
    }
  }

  class TopN(val n: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
    lazy val itemList = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("item-list", Types.of[ItemViewCount])
    )

    override def processElement(value: ItemViewCount,
                                ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                                out: Collector[String]): Unit = {
      itemList.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (item <- itemList.get) {
        allItems += item
      }
      itemList.clear() // 清空状态变量，gc

      val sortedItems = allItems
        .sortBy(-_.count) // 降序排列
        .take(n)

      val result = new StringBuilder
      result
        .append("=====================================\n")
        .append("时间：")
        .append(new Timestamp(timestamp - 100))
        .append("\n")

      for (i <- sortedItems.indices) {
        val cur = sortedItems(i)
        result
          .append("No")
          .append(i + 1)
          .append(": 商品ID = ")
          .append(cur.itemId)
          .append(" 浏览量 = ")
          .append(cur.count)
          .append("\n")
      }
      result
        .append("=====================================\n\n\n")

      Thread.sleep(1000)
      out.collect(result.toString)

    }
  }
}