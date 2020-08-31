package com.atguigu.flink.day02

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

/**
 * @author Skipper
 * @date 2020/08/31 
 * @desc  自定义JDBCSink将flink数据写入Mysql
 * create By Skipper 
 */
object JDBCSinkTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[SensorReading] = environment.fromCollection(List(
      SensorReading("1", System.currentTimeMillis(), 35.6),
      SensorReading("2", System.currentTimeMillis(), 29.6),
      SensorReading("4", System.currentTimeMillis(),33.6),
      SensorReading("5", System.currentTimeMillis(), 38.6),
      SensorReading("6", System.currentTimeMillis(), 24.1),
      SensorReading("7", System.currentTimeMillis(), 38.6)
    ))


    val customDataStream: DataStream[SensorReading] = environment.addSource(new MySource)
    customDataStream.addSink(new MyJdbcSink)
    environment.execute()
  }
}

class MyJdbcSink extends RichSinkFunction[SensorReading]{

  var conn : Connection = _
  var updateSql : PreparedStatement = _
  var insertSql : PreparedStatement = _
  override def close(): Unit = {
    updateSql.close()
    insertSql.close()
    conn.close()
  }

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/practise","root","root")
    updateSql = conn.prepareStatement("update sensor_temp set temperature = ? where id = ?")
    insertSql = conn.prepareStatement("insert into sensor_temp values (?,?)")
  }

  override def invoke(senSor: SensorReading, context: SinkFunction.Context[_]): Unit = {

    updateSql.setDouble(1,senSor.temperature)
    updateSql.setString(2,senSor.id)

    updateSql.execute()
    if (updateSql.getUpdateCount == 0){
      insertSql.setObject(1,senSor.id)
      insertSql.setObject(2,senSor.temperature)
      insertSql.execute()
    }

    println("数据 : " + senSor + "在数据库更新成功")
  }
}
