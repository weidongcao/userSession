package com.don.session.util;

/**
 * 字符串工具类
 * Created by caoweidong on 2017/2/5.
 */
public class StringUtils {
    /**
     * 判断字符串是否为空
     * @param str 字符串
     * @return 是否为空
     */
    public static boolean isEmpty(String str) {
        return str == null || "".equals(str);
    }

    /**
     * 判断字符串是否不为空
     * @param str 字符串
     * @return 是否不为空
     */
    public static boolean isNotEmpty(String str) {
        return str != null && !"".equals(str);
    }

    /**
     * 截断字符串两侧的逗号
     * @param str 字符串
     * @return 字符串
     */
    public static String trimComma(String str) {
        if(str.startsWith(",")) {
            str = str.substring(1);
        }
        if(str.endsWith(",")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }

    /**
     * 补全两位数字
     * @param str
     * @return
     */
    public static String fulfuill(String str) {
        if(str.length() == 2) {
            return str;
        } else {
            return "0" + str;
        }
    }

    /**
     * 从拼接的字符串中提取字段
     * @param str 字符串
     * @param delimiter 分隔符
     * @param kv_name 字段
     * @return 字段值
     */
    public static String getValueFromStringByName(String str, String delimiter, String kv_name) {
        String[] fields = str.split(delimiter);
        for(String concatField : fields) {
            if (concatField.split("=").length == 2) {
                String fieldName = concatField.split("=")[0];
                String fieldValue = concatField.split("=")[1];
                if(fieldName.equals(kv_name)) {
                    return fieldValue;
                }
            }
        }
        return null;
    }

    /**
     * 从拼接的key-value字符串中给字段设置值
     * @param str 字符串
     * @param innerDelimiter key和value之间的分隔符
     * @param outerDelimiter 不同key-value之间的分隔符
     * @param kv_name 字段名
     * @param val 新的field值
     * @return 字段值
     */
    public static String setValueToStringByName(String str, String innerDelimiter, String outerDelimiter, String kv_name, String val) {
        String[] fields = str.split(outerDelimiter);

        for(int i = 0; i < fields.length; i++) {
            String fieldName = fields[i].split(innerDelimiter)[0];
            if(fieldName.equals(kv_name)) {
                String concatField = fieldName + innerDelimiter + val;
                fields[i] = concatField;
                break;
            }
        }
        StringBuffer buffer = new StringBuffer("");
        for(int i = 0; i < fields.length; i++) {
            buffer.append(fields[i]);
            if(i < fields.length - 1) {
                buffer.append("|");
            }
        }

        return buffer.toString();
    }

}
