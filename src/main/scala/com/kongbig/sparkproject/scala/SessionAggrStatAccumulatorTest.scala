package com.kongbig.sparkproject.scala

import com.kongbig.sparkproject.constant.Constants
import com.kongbig.sparkproject.util.CustomStringUtils
import org.apache.spark.{AccumulatorParam, SparkConf, SparkContext}

/**
  * Describe: 
  * Author:   kongbig
  * Data:     2018/2/4.
  */
object SessionAggrStatAccumulatorTest {

  def main(args: Array[String]): Unit = {
    /**
      * Scala中自定义Accumulator，直接定义一个object继承AccumulatorParam
      */
    object SessionAggrStatAccumulatorScala extends AccumulatorParam[String] {
      /**
        * 实现zero方法负责返回一个初始值
        *
        * @param initialValue
        * @return
        */
      override def zero(initialValue: String): String = {
        Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s + "=0|" + Constants.TIME_PERIOD_4s_6s + "=0|" + Constants.TIME_PERIOD_7s_9s + "=0|" + Constants.TIME_PERIOD_10s_30s + "=0|" + Constants.TIME_PERIOD_30s_60s + "=0|" + Constants.TIME_PERIOD_1m_3m + "=0|" + Constants.TIME_PERIOD_3m_10m + "=0|" + Constants.TIME_PERIOD_10m_30m + "=0|" + Constants.TIME_PERIOD_30m + "=0|" + Constants.STEP_PERIOD_1_3 + "=0|" + Constants.STEP_PERIOD_4_6 + "=0|" + Constants.STEP_PERIOD_7_9 + "=0|" + Constants.STEP_PERIOD_10_30 + "=0|" + Constants.STEP_PERIOD_30_60 + "=0|" + Constants.STEP_PERIOD_60 + "=0"
      }

      /**
        * 实现一个累加方法
        *
        * @param v1
        * @param v2
        * @return
        */
      override def addInPlace(v1: String, v2: String): String = {
        if (v1 == "") {
          v2
        } else {
          // 从现有连接串中提取v2的值，进行累加，并重新设置
          val oldValue = CustomStringUtils.getFieldFromConcatString(v1, "\\|", v2);
          val newValue = Integer.valueOf(oldValue) + 1
          CustomStringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue))
        }
      }
    }

    // Spark上下文
    val conf = new SparkConf()
      .setAppName("SessionAggrStatAccumulatorTest")
      .setMaster("local")
    val sc = new SparkContext(conf);

    // 使用accumulator()()方法（柯里化），创建自定义的Accumulator
    val sessionAggrStatAccumulatorScala = sc.accumulator("")(SessionAggrStatAccumulatorScala)

    // 模拟使用自定义的Accumulator
    val arr = Array(Constants.TIME_PERIOD_1s_3s, Constants.TIME_PERIOD_4s_6s)
    val rdd = sc.parallelize(arr, 1)
    rdd.foreach {
      sessionAggrStatAccumulatorScala.add(_)
    }
    println(sessionAggrStatAccumulatorScala.value)
  }

}
