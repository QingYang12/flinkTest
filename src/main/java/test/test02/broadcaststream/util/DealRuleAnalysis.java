package test.test02.broadcaststream.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

/**
 * @author wanghao
 * @title: DealRuleAnalysis
 * @projectName flinkTest
 * @description: TODO
 * @date 2022/1/108:08 下午
 */
public class DealRuleAnalysis {

    public static String deal(String order,String config){
        JSONObject configjson=(JSONObject)JSON.parse(config);
        JSONObject orderjson=(JSONObject)JSON.parse(order);
        String test=configjson.getString("test");
        if(!StringUtils.isEmpty(test)){
            orderjson.put("judge",test);
        }

        String ordernew= orderjson.toJSONString();
        return ordernew;
    }
}
