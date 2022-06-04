import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

public class JSONUtils {
    public static boolean isjson(String log) {
        //判断log是否为json
        boolean flag = true;
        try {
            JSONObject.parseObject(log);
        } catch (JSONException e) {
            flag = false;
        }
        return flag;
    }
}
