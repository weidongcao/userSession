package com.don.session.spark;

import com.don.session.constant.Constants;
import com.don.session.util.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * Session聚合统计Accumulator
 * Created by caoweidong on 2017/2/15.
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

    /**
     * addInPlace和addAccumulator可以理解为是一样的
     * 这两个方法平方根就是实现
     * v1可能就是我们初始化的那个连接串
     * v2，就是我们在遍历session的时候，判断出某个Session对应折敬意，然后会用Constants.TIME_PERIOD_1s_3s
     * 所以，我们要做的事情就是在v1中找到v2对应的value，累加1，然后再更新回连接串里面去
     * @param t1
     * @param t2
     * @return
     */
    @Override
    public String addAccumulator(String t1, String t2) {
        return add(t1, t2);
    }

    @Override
    public String addInPlace(String r1, String r2) {
        return add(r1, r2);
    }

    /**
     * Zero方法主要用于数据的初始化
     * 就是就返回一个值，就是初始化中所有范围敬意的数据都是0
     * 各个范围敬意的统计数量的拼接还是采用key=value|key=value的连接串的格式
     * @param initialValue
     * @return
     */
    @Override
    public String zero(String initialValue) {
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    /**
     * Session统计计算逻辑
     * @param v1    连接串
     * @param v2    范围区间
     * @return      更新以后的连接串
     */
    private String add(String v1, String v2) {
        //校验：v1为空的话，直接返回v2
        if (StringUtils.isNotEmpty(v1)) {
            return v2;
        }

        //使用StringUtils工具类从v1中提取v2对应的值，并累加1
        String oldValue = StringUtils.getValueFromStringByName(v1, "\\|", v2);
        if (null != oldValue) {
            //将范围敬意原有的值累加1
            int newValue = Integer.valueOf(oldValue) + 1;
            //使用StringUtils工具类将v1中v2对应的值设置成新的累加后的值
            return StringUtils.setValueToStringByName(v1, ",","\\|", v2, String.valueOf(newValue));
        }

        return v1;
    }
}
