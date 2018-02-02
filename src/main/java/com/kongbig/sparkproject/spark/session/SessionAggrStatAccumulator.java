package com.kongbig.sparkproject.spark.session;

import com.kongbig.sparkproject.constant.Constants;
import com.kongbig.sparkproject.util.CustomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * Describe: session聚合统计Accumulator
 * <p>
 * 数据格式必须是可序列化，可以基于自定义的数据格式
 * <p>
 * Accumulator是Spark Code实用的技术
 * <p>
 * Author:   kongbig
 * Data:     2018/2/2.
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

    private static final long serialVersionUID = -4996005721644099405L;

    /**
     * zero方法，主要用于数据的初始化
     * 这里我们就返回一个值，就是初始化中，所有范围区间的数量，都是0。
     * 各个范围区间的统计数量的拼接，还是使用key=value|key=value的连接串的格式
     *
     * @param v1
     * @return
     */
    @Override
    public String zero(String v1) {
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
     * addInPlace和addAccumulator现在理解为一样
     * <p>
     * 这两个方法，其实主要就是实现，v1可能就是我们初始化的那个连接串
     * v2，就是我们在遍历session的时候，判断出某个session对应的区间，然后会用Constants.TIME_PERIOD_1s_3s
     * 所以，我们，要做的事情就是
     * 在v1中，找到v2对应的value，累加1，然后再更新回连接串里面去
     *
     * @param v1
     * @param v2
     * @return
     */
    @Override
    public String addInPlace(String v1, String v2) {
        return add(v1, v2);
    }

    @Override
    public String addAccumulator(String v1, String v2) {
        return add(v1, v2);
    }

    /**
     * session统计计算逻辑
     *
     * @param v1 连接串
     * @param v2 范围区间
     * @return 返回更新以后的连接串
     */
    private String add(String v1, String v2) {
        // 校验：v1为空的话，直接返回v2
        if (StringUtils.isEmpty(v1)) {
            return v2;
        }

        // 使用CustomStringUtils工具类，从v1中，提取v2对应的值，并累加1。
        String oldValue = CustomStringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if (StringUtils.isNotEmpty(oldValue)) {
            // 将范围区间原有的值，累加1
            int newValue = Integer.valueOf(oldValue) + 1;

            // 使用CustomStringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
            return CustomStringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
        }
        return v1;
    }

}
