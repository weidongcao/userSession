package com.don.session.util;

/**
 * 校验工具类
 * Created by caoweidong on 2017/2/5.
 */
public class ValidUtils {
    /**
     * 校验数据中的指定字段，是否在指定范围内
     * @param data 数据
     * @param dataField 数据字段
     * @param parameter 参数
     * @param startFieldName 起始参数字段
     * @param endFieldName 结束参数字段
     * @return 校验结果
     */
    public static boolean validBetween(String data, String dataField,
                                       String parameter, String startFieldName, String endFieldName) {
        String startFieldValueStr = StringUtils.getValueFromStringByName(
                parameter, "\\|", startFieldName);
        String endFieldValueStr = StringUtils.getValueFromStringByName(
                parameter, "\\|", endFieldName);
        if(startFieldValueStr == null || endFieldValueStr == null) {
            return true;
        }

        int startFieldValue = Integer.valueOf(startFieldValueStr);
        int endFieldValue = Integer.valueOf(endFieldValueStr);

        String dataFieldStr = StringUtils.getValueFromStringByName(
                data, "\\|", dataField);
        if(dataFieldStr != null) {
            int dataFieldValue = Integer.valueOf(dataFieldStr);
            if(dataFieldValue >= startFieldValue &&
                    dataFieldValue <= endFieldValue) {
                return true;
            } else {
                return false;
            }
        }

        return false;
    }

    /**
     * 校验数据中的指定字段，是否有值与参数字段的值相同
     * @param data 数据
     * @param dataField 数据字段
     * @param parameter 参数
     * @param paramField 参数字段
     * @return 校验结果
     */
    public static boolean validIn(String data, String dataField,
                                  String parameter, String paramField) {
        String paramFieldValue = StringUtils.getValueFromStringByName(
                parameter, "\\|", paramField);
        if(paramFieldValue == null) {
            return true;
        }
        String[] paramFieldValueSplited = paramFieldValue.split(",");

        String dataFieldValue = StringUtils.getValueFromStringByName(
                data, "\\|", dataField);
        if(dataFieldValue != null) {
            String[] dataFieldValueSplited = dataFieldValue.split(",");

            for(String singleDataFieldValue : dataFieldValueSplited) {
                for(String singleParamFieldValue : paramFieldValueSplited) {
                    if(singleDataFieldValue.equals(singleParamFieldValue)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * 校验数据中的指定字段，是否在指定范围内
     * @param data 数据
     * @param dataField 数据字段
     * @param parameter 参数
     * @param paramField 参数字段
     * @return 校验结果
     */
    public static boolean validEqual(String data, String dataField,
                                     String parameter, String paramField) {
        String paramFieldValue = StringUtils.getValueFromStringByName(
                parameter, "\\|", paramField);
        if(paramFieldValue == null) {
            return true;
        }

        String dataFieldValue = StringUtils.getValueFromStringByName(
                data, "\\|", dataField);
        if(dataFieldValue != null) {
            if(dataFieldValue.equals(paramFieldValue)) {
                return true;
            }
        }

        return false;
    }
}
