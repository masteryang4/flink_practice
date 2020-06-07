package com.atguigu.datastreamapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)

    val conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build()

    stream.addSink(new RedisSink[SensorReading](conf, new RedisExampleMapper))

    env.execute()

  }

  class RedisExampleMapper extends RedisMapper[SensorReading] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(
        RedisCommand.HSET,
        "sensor_temperature")
    }

    override def getKeyFromData(t: SensorReading): String = t.id

    override def getValueFromData(t: SensorReading): String = {
      t.temperature.toString
    }
  }
}