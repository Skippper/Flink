package com.atguigu.flink.day02

import java.util.Properties

import javafx.beans.property.SimpleIntegerProperty
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011, Kafka011TableSink}

/**
 * @author Skipper
 * @date 2020/08/31 
 * @desc
 * create By Skipper 
 */
object SinkTest {
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
    //kafka数据源
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    val kafkaDataStream: DataStream[String] = environment.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    kafkaDataStream.addSink(new FlinkKafkaProducer011[String]("hadoop102:9092","sensor_receiver",new SimpleStringSchema()))
    //写入文件
    //dataStream.writeAsCsv("E:\\Shangguigu\\Codes\\Flink\\src\\main\\resources\\sensor.out")


    environment.execute("")
  }
}
