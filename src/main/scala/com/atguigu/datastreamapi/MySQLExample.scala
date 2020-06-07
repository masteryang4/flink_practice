package com.atguigu.datastreamapi

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object MySQLExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)
    stream.addSink(new MyJdbcSink)

    env.execute()

  }

  // TwoPhaseCommitSinkFunction
  class MyJdbcSink extends RichSinkFunction[SensorReading] {
    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var updateStmt: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection(
        "jdbc:mysql://localhost:3306/test",
        "root","root"
      )

      insertStmt = conn.prepareStatement(
        "INSERT INTO temperatures (sensor, temp) VALUES (?, ?)"
      )

      updateStmt = conn.prepareStatement(
        "UPDATE temperatures SET temp = ? WHERE sensor = ?"
      )
    }

    override def invoke(value: SensorReading,
                        context: SinkFunction.Context[_]): Unit = {
      updateStmt.setDouble(1, value.temperature)
      updateStmt.setString(2, value.id)
      updateStmt.execute()

      if (updateStmt.getUpdateCount == 0) {
        insertStmt.setString(1, value.id)
        insertStmt.setDouble(2, value.temperature)
        insertStmt.execute()
      }
    }

    override def close(): Unit = {
      insertStmt.close()
      updateStmt.close()
      conn.close()
    }
  }
}