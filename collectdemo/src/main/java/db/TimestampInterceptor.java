package db;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TimestampInterceptor implements Interceptor {//TODO:f3拦截器，提取时间戳
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //把eventzhong时间数据放入header中
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        JSONObject jsonObject = JSONObject.parseObject(log);
        Long ts = jsonObject.getLong("ts");
        String stringts = ts * 1000 + "";
        Map<String, String> headers = event.getHeaders();
        headers.put("timestamp", stringts);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TimestampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
