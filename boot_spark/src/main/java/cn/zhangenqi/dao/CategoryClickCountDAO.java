package cn.zhangenqi.dao;

import cn.zhangenqi.domain.CategoryClickCount;
import cn.zhangenqi.utils.HBaseUtil;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 17:01 2018/7/5
 * @Description:
 */

@Service
public class CategoryClickCountDAO {
    public List<CategoryClickCount> query(String day) throws IOException {

        List<CategoryClickCount> list = new ArrayList<>();
        Map<String,Long> map = HBaseUtil.getInstance().query("category_clickcount",day);
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            CategoryClickCount categoryClickCount = new CategoryClickCount();
            categoryClickCount.setName(entry.getKey());
            categoryClickCount.setValue(entry.getValue());
            list.add(categoryClickCount);
        }
        return list;
    }
/*    public static void main(String[] args) throws IOException {
        CategoryClickCountDAO dao = new CategoryClickCountDAO();
        List<CategoryClickCount> list = dao.query("2017");
        for (CategoryClickCount c : list) {
            System.out.println(c.getValue());
        }
    }*/
}
