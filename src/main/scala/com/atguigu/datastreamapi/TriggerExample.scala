package com.atguigu.datastreamapi

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TriggerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val readings = env
      .addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .trigger(new OneSecondIntervalTrigger)
      .process(new MyWindow)

    readings.print()
    env.execute()
  }

  class OneSecondIntervalTrigger extends Trigger[SensorReading, TimeWindow] {
    override def onElement(element: SensorReading,
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: TriggerContext): TriggerResult = {

      // 初始值为false
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )

      // 只为第一个元素注册定时器
      // 例如第一个元素的时间戳是：1234ms
      // 那么t = ？
      if (!firstSeen.value()) {
        val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        ctx.registerEventTimeTimer(t) // 注册一个2000ms的定时器
        ctx.registerEventTimeTimer(window.getEnd) // 注册一个窗口结束时间的定时器
        firstSeen.update(true)
      }
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long,
                                  window: TimeWindow,
                                  ctx: TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(timestamp: Long,
                             window: TimeWindow,
                             ctx: TriggerContext): TriggerResult = {
      if (timestamp == window.getEnd) {
        TriggerResult.FIRE_AND_PURGE
      } else {
        val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        if (t < window.getEnd) {
          ctx.registerEventTimeTimer(t)
        }
      }
      TriggerResult.FIRE
    }

    override def clear(window: TimeWindow,
                       ctx: Trigger.TriggerContext): Unit = {
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen", Types.of[Boolean])
      )
      firstSeen.clear()
    }
  }

  class MyWindow extends ProcessWindowFunction[SensorReading, String, String, TimeWindow] {
    override def process(key: String,
                         context: Context, elements: Iterable[SensorReading],
                         out: Collector[String]): Unit = {
      out.collect("现在窗口有" + elements.size + "个元素")
    }
  }
}