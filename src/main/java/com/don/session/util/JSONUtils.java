package com.don.session.util;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by caoweidong on 2017/2/5.
 */
public class JSONUtils {
    /**
     * 从JSON对象中提取参数
     *
     * @param obj JSON对象
     * @return 参数
     */
    public static String getParam(JSONObject obj, String field) {
        try {
            JSONArray array = obj.getJSONArray(field);
            if (array != null && array.length() > 0) {
                return array.getString(0);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }
}
