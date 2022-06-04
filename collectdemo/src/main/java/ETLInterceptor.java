import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * 1.继承接口
 * 2.重写4个抽象方法
 * 3.静态内部类builder实现接口builder
 */

public class ETLInterceptor implements Interceptor {//todo：f1拦截器，过滤非json数据
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //todo:过滤enent中的数据是否为json
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        //判断log是否为json
        return JSONUtils.isjson(log) ? event:null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        Iterator<Event> iterator = list.iterator();
        while (iterator.hasNext()) {
            if (iterator.next()==null) {
                iterator.remove();
            }
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
