package cn.zhangenqi.dao

import cn.zhangenqi.domain.{CategoryClickCount, CategorySearchClickCount}
import cn.zhangenqi.util.HBaseUtil
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer


object CategorySearchClickCountDao{
  val tableName = "category_search_count"
  val columnFamily = "info"
  val qualifier = "click_count"

  /**
    * 保存数据到HBase
    */
  def save(list: ListBuffer[CategorySearchClickCount]): Unit = {
    val table = HBaseUtil.getInstance().getHTable(tableName)
    for (ele <- list) {
      /*
      这是计数器操作  最后一个是步长
      没有则创建 否则 就累加
       */
      table.incrementColumnValue(Bytes.toBytes(ele.day_search_categpry),
        Bytes.toBytes(columnFamily),
        Bytes.toBytes(qualifier),
        ele.clickCount)
    }
  }

  /**
    * 获取数据
    */
  def count(day_categoryId: String): Long = {
    val table = HBaseUtil.getInstance().getHTable(tableName)
    //都是row key
    val get = new Get(Bytes.toBytes(day_categoryId))
    val values = table.get(get).getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier))

    if (values == null) {
      0L
    } else {
      Bytes.toLong(values)
    }
  }

  def main(args: Array[String]): Unit = {
    val l = new ListBuffer[CategorySearchClickCount]
    l.append(CategorySearchClickCount("20180707_1_1", 100))
    l.append(CategorySearchClickCount("20180707_2_2", 100))
    l.append(CategorySearchClickCount("20180707_3_3", 100))
    save(l)
    print(count("20180707_1_1"))
  }
}
