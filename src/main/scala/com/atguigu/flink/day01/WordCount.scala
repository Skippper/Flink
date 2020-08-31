package com.atguigu.flink.day01

import java.io.{FileInputStream, InputStream}
import java.net.URL

import org.apache.flink.api.scala._

/**
 * @author Skipper
 * @date 2020/08/29
 * @desc
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    //1.创建一个批处理环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

   val FilePath = "E:\\Shangguigu\\Codes\\Flink\\src\\main\\resources\\hello.txt"

    val dataSet: DataSet[String] = environment.readTextFile(FilePath)

    val res: AggregateDataSet[(String, Int)] = dataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    res.print()
  }
}
