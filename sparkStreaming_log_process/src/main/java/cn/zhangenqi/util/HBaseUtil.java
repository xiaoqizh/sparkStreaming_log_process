package cn.zhangenqi.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

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
    public HTable getHTable(String tableName) {
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
     * 测试
     */
    public static void main(String[] args) {
        String tableName = "category_clickcount";
        String rk = "20171122_1";
        String columnfamily = "info";
        HBaseUtil instance1 = HBaseUtil.getInstance();
        String column = "category_click_count";
        String value = "100";
        instance1.put(tableName, rk, columnfamily, column, value);

    }

}
