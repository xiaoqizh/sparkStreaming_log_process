package cn.zhangenqi.app

import cn.zhangenqi.dao.{CategoryClickCountDao, CategorySearchClickCountDao}
import cn.zhangenqi.domain.{CategoryClickCount, CategorySearchClickCount, ClickLog}
import cn.zhangenqi.util.DateUtil
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  *这个的运行速度太慢了  需要运行好几分钟呢
  */

object StatStreamingApp {
  def main(args: Array[String]): Unit = {
//    配置streaming
    val ssc = new StreamingContext("local","StatStreamingApp",Seconds(5))
//    配置kafka参数
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "192.168.247.128:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
      )

    val topics = Array("flumeTopic")

    val logs = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    ).map(_.value)

    /*
    进行数据清洗
    100.132.143.167	2018-07-03 21:54:08	"GET www/3 HTTP/1.0"	-	200
     */
    val cleanLog =logs.map(line => {
      var  infos = line.split("\t")
      //得到 www/3
      val url = infos(2).split(" ")(1)
      var category = 0
      if (url.startsWith("www/")){
        category = url.split("/")(1).toInt
      }
      ClickLog(infos(0),DateUtil.parse(infos(1)),category,infos(3),infos(4).toInt)
    })
      .filter(log => log.category!=0)

    cleanLog.print()
    //每个类别每天的点击量  时间 类别 作为row key
    //这一步的效果就是 (row key,1)
    cleanLog.map( log => {
       (log.time.substring(0,8)+log.category,1)
    }).reduceByKey(_+_).foreachRDD( rdd=>{
        rdd.foreachPartition( parts =>{
        val list = new ListBuffer[CategoryClickCount]
        //对于每个partition中的每个数组
        parts.foreach(pair =>{
          list.append(CategoryClickCount(pair._1+"_",pair._2))
        })
        CategoryClickCountDao.save(list)
      })
    })

    /*
     每个栏目下的流量
     20171122_1_1
     第一个1 是渠道
     第二个1 是类别
     create "cate

     进行数据清洗
     29.30.187.143	2018-07-04 19:48:33	"GET www/4 HTTP/1.0"	https://search.yahoo.com/search?p=我的体育老师	200
     处理成   20180704_https://search.yahoo.com/search?p=我的体育老师_4
     */
    cleanLog.map(log => {
      val url = log.refer.replace("//","/")
      val splits = url.split("/")
      var host =" "
      if(splits.length>2) {
        host=splits(1)
      }
      (host,log.time,log.category)
    })
      .filter(x => x._1 !="")
      .map(x => {
        (x._2.substring(0,8)+"_"+x._1+"_"+x._3,1)
      })
      .reduceByKey(_+_)
      .foreachRDD( rdd=> {
        rdd.foreachPartition(values => {
          val list = new ListBuffer[CategorySearchClickCount]
          values.foreach(x => {
            list.append(CategorySearchClickCount(x._1, x._2))
          })
          CategorySearchClickCountDao.save(list)
        })
      })
    // logs.print
    ssc.start
    ssc.awaitTermination
  }
}
