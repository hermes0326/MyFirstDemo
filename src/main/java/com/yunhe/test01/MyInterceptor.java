package com.yunhe.test01;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @Auther: Li
 * @Date: 2023/6/20 16:00
 * @Desc:
 **/
public class MyInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }


    @Override
    public Event intercept(Event event) {

        // 解析event对象信息 成 json对象
        byte[] body = event.getBody();
        String s = new String(body, StandardCharsets.UTF_8);
        JSONObject jsonObject = JSON.parseObject(s);

        //将解析好的json对象封装到map中 再将map注入到list中
        ArrayList<LinkedHashMap<String, String>> linkedHashMaps = new ArrayList<>();

        //获取字段并封装
        String items = jsonObject.getString("items");
        JSONArray jsonArray = jsonObject.getJSONArray("items");
        for (Object o : jsonArray) {
            JSONObject json = JSON.parseObject(o.toString());
            String item_type = json.getString("item_type");
            String active_time = json.getString("active_time");
            String host = jsonObject.getString("host");
            String user_id = jsonObject.getString("user_id");

            LinkedHashMap<String, String> linkedHashMap = new LinkedHashMap<>();
            linkedHashMap.put("user_id",user_id);
            linkedHashMap.put("host",host);
            linkedHashMap.put("active_time",active_time);
            linkedHashMap.put("item_type",item_type);

            linkedHashMaps.add(linkedHashMap);
        }
        //将清洗后的信息注入到event对象中
       event.setBody(linkedHashMaps.toString().getBytes());

        return event;
    }

    /**
       * @Description:  调用写好的单个处理代码
       * @Param:
       * @return:
       */
    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList<Event> arrayList = new ArrayList<>();

        for (Event event : list) {
            arrayList.add(intercept(event));
        }
        Thread thread = new Thread();
        return arrayList;
    }

    @Override
    public void close() {

    }

    /*
    * 使用内部类 提供自定义拦截器接口
    * */
    public static class buildEvent implements Builder{

        @Override
        public Interceptor build() {

            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
