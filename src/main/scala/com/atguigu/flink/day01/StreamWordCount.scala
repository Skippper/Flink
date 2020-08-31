package com.atguigu.flink.day01

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.AggregateDataSet
import org.apache.flink.streaming.api.scala._





/**
 * @author Skipper
 * @date 2020/08/29
 * @desc
 * create By Skipper 
 */
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    val params: ParameterTool =  ParameterTool.fromArgs(args)

    val host: String = params.get("host")
    val port: Int = params.get("port").toInt

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataSTream: DataStream[String] = environment.socketTextStream(host, port)

    val resDStream: DataStream[(String, Int)] = dataSTream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)


    resDStream.print()
    environment.execute()
  }
}
