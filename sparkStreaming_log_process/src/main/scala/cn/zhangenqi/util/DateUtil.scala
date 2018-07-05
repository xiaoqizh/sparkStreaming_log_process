package cn.zhangenqi.util

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat


object DateUtil {

  val  from = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val to = FastDateFormat.getInstance("yyyyMMdd")

  /**
    * @return 当前时间的时间戳
    */
    def getTime(from_time:String)={
     from.parse(from_time).getTime
    }

    def parse(time:String)={
      to.format(new Date(getTime(time)))
    }

  def main(args: Array[String]): Unit = {
    print(parse("2018-07-03 21:54:08"))

  }
}
