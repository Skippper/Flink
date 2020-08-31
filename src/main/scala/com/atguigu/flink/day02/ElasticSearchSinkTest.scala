package com.atguigu.flink.day02

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
 * @author Skipper
 * @date 2020/08/31 
 * @desc  flink数据写入elasticSearch
 * create By Skipper 
 */
object ElasticSearchSinkTest {

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

    var httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102",9200))


    var esSink: ElasticsearchSink[SensorReading] = new ElasticsearchSink.Builder[SensorReading](httpHosts, new MyEsSinkFunction).build()
    dataStream.addSink(esSink)
    environment.execute()
  }
}

class MyEsSinkFunction extends ElasticsearchSinkFunction[SensorReading]{

  override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
    var sourceData = new util.HashMap[String, String]()
    sourceData.put("data",element.toString)

    val indexRequest: IndexRequest = Requests.indexRequest()
      .index("sensor")
      .`type`("_doc")
      .id(element.id)
      .source(sourceData)
    indexer.add(indexRequest)

    println("数据 : " + element + " 保存成功")
  }
}
