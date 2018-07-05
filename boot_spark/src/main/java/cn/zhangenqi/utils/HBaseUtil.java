package cn.zhangenqi.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 16:16 2018/7/4
 * @Description:
 */

public class HBaseUtil {

    private HBaseAdmin hBaseAdmin = null;
    private Configuration configuration = null;
    private static HBaseUtil instance = new HBaseUtil();

    private HBaseUtil() {
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","xiaoqizh:2184");
        configuration.set("hbase.rootdir","hdfs://xiaoqizh:9000/hbase2");
        try {
            hBaseAdmin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static  HBaseUtil getInstance() {
        return instance;
    }

    /**
     * 获取HTable实例
     */
    public    HTable getHTable(String tableName) {
        HTable table = null;
        try {
            table = new HTable(configuration, TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     *
     * @param tableName 表明
     * @param rowKey row key
     * @param columnfamily 列蔟
     * @param column 列
     * @param value 值
     */
    public void put(String tableName, String rowKey, String  columnfamily, String column, String value) {
        HTable table = getHTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(columnfamily),
                Bytes.toBytes(column),
                Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 根据表名输入条件获取 Hbase 的记录数
     * 这些东西都是可以从官网上查
     * @param tableName 查询的表名
     * @param condition 20180703
     * @return  存放结果集
     * @throws IOException
     */
    public Map<String, Long> query(String tableName, String condition) throws IOException {
        Map<String, Long> map = new HashMap<>(2);
        HTable table = getHTable(tableName);
        String cf = "info";
        String qualifier = "click_count";
        Scan scan = new Scan();
        Filter filter = new PrefixFilter(Bytes.toBytes(condition));
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            //这是row key
            String row = Bytes.toString(result.getRow());
            long clickCount = Bytes.toLong(result.getValue(cf.getBytes(), qualifier.getBytes()));
            map.put(row, clickCount);
        }
        return map;
    }


}
