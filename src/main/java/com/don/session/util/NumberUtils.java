package com.don.session.util;

import java.math.BigDecimal;

/**
 * 数字格式工具类
 * Created by caoweidong on 2017/2/5.
 */
public class NumberUtils {
    /**
     * 将数字格式化为指定位数的小数
     *
     * @param num
     * @param scale 四舍五入的位数
     * @return 格式化的小数
     */
    public static double getFormatDouble(double num, int scale) {
        BigDecimal bd = new BigDecimal(num);
        return bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }
}
