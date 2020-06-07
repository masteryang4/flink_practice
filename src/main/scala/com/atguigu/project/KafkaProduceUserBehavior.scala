package com.atguigu.project

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProduceUserBehavior {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems1")
  }

  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    val producer = new KafkaProducer[String, String](props)

    val bufferedSource = io.Source.fromFile("/Users/yuanzuo/Desktop/Flink1125SH/src/main/resources/UserBehavior.csv")
    for (line <- bufferedSource.getLines) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}