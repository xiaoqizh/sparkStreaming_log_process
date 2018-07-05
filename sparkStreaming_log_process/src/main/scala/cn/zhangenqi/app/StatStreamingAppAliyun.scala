package cn.zhangenqi.app

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StatStreamingAppAliyun {
  def main(args: Array[String]): Unit = {
//    配置streaming
    val ssc = new StreamingContext("local[2]","StatStreamingApp",Seconds(5))
//    配置kafka参数
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "********:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test11",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
      )

    val topics = Array("spark20180703")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    ).map(_.value)

    stream.print
    ssc.start
    ssc.awaitTermination
  }
}
