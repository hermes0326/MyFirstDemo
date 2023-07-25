package com.yunhe.collector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import org.apache.commons.codec.binary.Base64;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @Auther: Li
 * @Date: 2023/6/30 12:08
 * @Desc:
 **/
public class SwitchText implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        //1. 获取数据本身
        String text = new String(event.getBody());
        //2. 切割
        String[] textArray = text.split("-");

        byte[] body = null;
        //3. 判断
        if (textArray.length == 2) {
            try {
                //4. 获取到解码的字符串
                String meta = new String(Base64.decodeBase64(textArray[0]));
                String content = new String(Base64.decodeBase64(textArray[1]));

                //5. 将json的字符串转换为json的对象:ctime、project、ip
                JSONObject jsonMeta = JSONObject.parseObject(meta);

                //6. 获取到字段: ctime: 111111.111
                String ctime = JSONPath.eval(jsonMeta, "$.ctime").toString();
                DateFormat fmt = new SimpleDateFormat("yyyyMMdd");
                ctime = fmt.format(Double.parseDouble(ctime)); // 20220622

                //7. 将ctime的字段插入到flume的event的header中
                event.getHeaders().put("ctime", ctime);

                //8. 解析content
                JSONObject jsonContent = JSONObject.parseObject(content);

                //9. 将jsonContent和jsonMeta对象合并为一个json对象
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("ctime", JSONPath.eval(jsonMeta, "$.ctime"));
                jsonObject.put("project", JSONPath.eval(jsonMeta, "$.project"));
                jsonObject.put("content",JSONPath.eval(jsonContent, "$.content"));

                //10. 复制body数组
                body = jsonObject.toString().getBytes();
            }catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
        //11. 设置event的值
        event.setBody(body);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List<Event> newList = new ArrayList<>();

        for (Event event : list) {
            Event newEvent = intercept(event);
            newList.add(newEvent);
        }
        return newList;
    }

    @Override
    public void close() {

    }

    // 内部类，用于创建interceptor
    public static class BuilderEvent implements Builder {

        @Override
        public Interceptor build() {
            return new SwitchText();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
