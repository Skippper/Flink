package com.atguigu.flink.day02

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


/**
 * @author Skipper
 * @date 2020/08/31 
 * @desc  flink数据写入Redis
 * create By Skipper 
 */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    /*val dataStream: DataStream[SensorReading] = environment.fromCollection(List(
      SensorReading("sensor_1", System.currentTimeMillis(), 35.6),
      SensorReading("sensor_1", System.currentTimeMillis(), 35.7),
      SensorReading("sensor_1", System.currentTimeMillis(), 35.9),
      SensorReading("sensor_2", System.currentTimeMillis(), 29.6),
      SensorReading("sensor_4", System.currentTimeMillis(),33.6),
      SensorReading("sensor_5", System.currentTimeMillis(), 38.6),
      SensorReading("sensor_6", System.currentTimeMillis(), 24.1),
      SensorReading("sensor_7", System.currentTimeMillis(), 38.6)
    ))*/
    val myDataStream: DataStream[SensorReading] = environment.addSource(new MySource)
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop102")
      .setPort(6379).build()

    myDataStream.addSink(new RedisSink[SensorReading](config,new MyRedisMapper))
    environment.execute()
  }
}
class MyRedisMapper extends RedisMapper[SensorReading]{

  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET,"sensor_temperature")
  }

  override def getKeyFromData(data: SensorReading): String = data.id

  override def getValueFromData(data: SensorReading): String = data.temperature.toString
}
