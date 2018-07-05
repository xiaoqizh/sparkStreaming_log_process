package cn.zhangenqi.web;

import cn.zhangenqi.dao.CategoryClickCountDAO;
import cn.zhangenqi.domain.CategoryClickCount;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: xiaoqiZh
 * @Date: Created in 20:54 2018/7/4
 * @Description:
 */

@RestController
public class Boot {

    private static Map<String,String> categoryList = new HashMap<>();

    @Autowired
    private CategoryClickCountDAO categoryClickCountDAO;

    static {
        categoryList.put("1","偶像爱情");
        categoryList.put("2","宫斗谋权");
        categoryList.put("4","玄幻史诗");
        categoryList.put("6", "都市生活");
        categoryList.put("3", "罪案谍战");
        categoryList.put("13", "历险科幻");
    }

    @PostMapping(value = "analysis")
    public List<CategoryClickCount> analysis() throws IOException {
        List<CategoryClickCount> list = categoryClickCountDAO.query("201807031");
        for(CategoryClickCount model:list){
            //得到类别 因为存储的是数字 不是具体的类别
            //引起那面饭返回 key 是
            model.setName(categoryList.get(model.getName().substring(9)));
        }
        return list;
    }

}
