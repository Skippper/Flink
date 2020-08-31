package com.atguigu.flink.day02

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * @author Skipper
 * @date 2020/08/31 
 * @desc  Flink Source 数据源
 * create By Skipper 
 */
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object Sensor {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //从集合读取
    val resDataStream: DataStream[SensorReading] = environment.fromCollection(List(
      SensorReading("1", System.currentTimeMillis(), 35.6),
      SensorReading("2", System.currentTimeMillis(), 29.6),
      SensorReading("4", System.currentTimeMillis(),33.6),
      SensorReading("5", System.currentTimeMillis(), 38.6),
      SensorReading("6", System.currentTimeMillis(), 24.1),
      SensorReading("7", System.currentTimeMillis(), 38.6)
    ))

    //从文件读取
    val filePath = "E:\\Shangguigu\\Codes\\Flink\\src\\main\\resources\\sensor.txt"

    val fileDataStream: DataStream[String] = environment.readTextFile(filePath)

    //从kafka数据源读取
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")

    val kafkaDataStream: DataStream[String] = environment.addSource(new FlinkKafkaConsumer011[String](
      "sensor", new SimpleStringSchema(), properties))

    //自定义Source
    val myDataStream: DataStream[SensorReading] = environment.addSource(new MySource)

    //分流
    val splitStream: SplitStream[SensorReading] = resDataStream.split(SensorReading => {
      if (SensorReading.temperature > 30) Seq("high")
      else Seq("low")
    })

    val highDataStream: DataStream[SensorReading] = splitStream.select("high")
    val lowDataStream: DataStream[SensorReading] = splitStream.select("low")

    //双流连接
    val warning: DataStream[(String, Double)] = highDataStream.map(SensorReading => {
      (SensorReading.id, SensorReading.temperature)
    })

    val connectedStream: ConnectedStreams[(String, Double), SensorReading] = warning.connect(lowDataStream)

    val coMap: DataStream[Product] = connectedStream.map(
      warningData => (warningData._1, warningData._2, "hot_weather"),
      lowData => (lowData.id, "healthy")
    )
    coMap.print()
//     highDataStream.print()
//    lowDataStream.print()
    ///resDataStream.print()
   // fileDataStream.print()

    //kafkaDataStream.print()

   // myDataStream.print()
    environment.execute()
  }
}
