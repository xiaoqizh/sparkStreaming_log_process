package cn.zhangenqi.domain;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 17:00 2018/7/5
 * @Description:
 */

public class CategoryClickCount {

    private String name;
    private long value;

    public void setName(String name) {
        this.name = name;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    public String getName() {
        return name;
    }
}

