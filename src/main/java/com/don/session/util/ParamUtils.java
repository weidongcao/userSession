package com.don.session.util;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * 参数工具类
 * Created by caoweidong on 2017/2/5.
 */
public class ParamUtils {
    /**
     * 从命令行参数中提取任务ID
     *
     * @param args 命令行参数
     * @return 任务ID
     */
    public static Long getLongFromArgs(String[] args) {
        try {
            if (args != null && args.length > 0) {
                return Long.valueOf(args[0]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return  null;
    }

    /**
     * 从JSON对象中提取参数
     */
    public static String getParam(JSONObject jsonObject, String field) {
        JSONArray jsonArray = null;
        try {
            jsonArray = jsonObject.getJSONArray(field);
            if (jsonArray != null && jsonArray.length() > 0) {
                return jsonArray.getString(0);
            }
        } catch (JSONException e) {
        }
        return null;
    }
}
