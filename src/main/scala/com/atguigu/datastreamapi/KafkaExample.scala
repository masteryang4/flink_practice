package com.atguigu.datastreamapi

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object KafkaExample {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty(
      "bootstrap.servers",
      "localhost:9092"
    )
    properties.setProperty(
      "group.id",
      "consumer-group"
    )
    properties.setProperty(
      "key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    properties.setProperty(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    properties.setProperty("auto.offset.reset", "latest")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(
        new FlinkKafkaConsumer011[String](
          "test",
          new SimpleStringSchema(),
          properties
        )
      )

    stream.print()

    stream.addSink(
      new FlinkKafkaProducer011[String](
        "localhost:9092",
        "test",
        new SimpleStringSchema()))
    env.execute()
  }
}