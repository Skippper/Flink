package com.atguigu.flink.day02

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.collection.immutable
import scala.util.Random

/**
 * @author Skipper
 * @date 2020/08/31 
 * @desc
 * create By Skipper 
 */
class MySource extends SourceFunction[SensorReading]{

  var running : Boolean = true;
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {


    while (running){
      //
      val random = new Random()

      var curTemp = 1.to(10).map(i => ("sensor_" + i, 65 + random.nextGaussian() * 20))

        curTemp = curTemp.map(
          t => (t._1, t._2 + random.nextGaussian() )
        )
        //当前时间戳
        val cutTms: Long = System.currentTimeMillis()

        //遍历发送数据
        curTemp.foreach{
          case (id,tmp) => {
            ctx.collect(SensorReading(id,cutTms,tmp))
          }
        }

        Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = running = false
}
