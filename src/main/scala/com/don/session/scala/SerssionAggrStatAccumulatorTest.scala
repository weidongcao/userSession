package com.don.session.scala

import com.don.session.constant.Constants
import com.don.session.util.StringUtils
import org.apache.spark.{AccumulatorParam, SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/3/2.
  */
object SerssionAggrStatAccumulatorTest {
  def main(args: Array[String]): Unit = {
    /**
      * Scala中自定义Accumulator
      * 使用Object，直接定义一个伴生对象即可
      * 需要实现AccumulatorParam接口，并使用[]语法，定义输入输出的数据格式
      */
    object SerssionAggrStatAccumulator extends AccumulatorParam[String] {


      /**
        * 实现zero方法
        * 负责返回一个初始值
        *
        * @param initialValue
        * @return
        */
      override def zero(initialValue: String): String = {
        Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s + "=0|" + Constants.TIME_PERIOD_4s_6s + "=0|" + Constants.TIME_PERIOD_7s_9s + "=0|" + Constants.TIME_PERIOD_10s_30s + "=0|" + Constants.TIME_PERIOD_30s_60s + "=0|" + Constants.TIME_PERIOD_1m_3m + "=0|" + Constants.TIME_PERIOD_3m_10m + "=0|" + Constants.TIME_PERIOD_10m_30m + "=0|" + Constants.TIME_PERIOD_30m + "=0|" + Constants.STEP_PERIOD_1_3 + "=0|" + Constants.STEP_PERIOD_4_6 + "=0|" + Constants.STEP_PERIOD_7_9 + "=0|" + Constants.STEP_PERIOD_10_30 + "=0|" + Constants.STEP_PERIOD_30_60 + "=0|" + Constants.STEP_PERIOD_60 + "=0|"
      }

      /**
        * 实现累加方法
        */
      override def addInPlace(r1: String, r2: String): String = {
        //如果初始化值为空，返回v2
        if (r1 == "") {
          r2
        } else {
          //从现有的连接串中提取r2对应的值
          val oldValue = StringUtils.getValueFromStringByName(r1, "\\|", r2)
          //累加1
          val newValue = Integer.valueOf(oldValue) + 1
          //给连接串中的r2设置打的累加后的值
          StringUtils.setValueToStringByName(r1, "=", "\\|", r2, String.valueOf(newValue))
        }
      }
    }

    //创建Spark上下文
    val conf = new SparkConf()
      .setAppName("SerssionAggrStatAccumulatorTest")
      .setMaster("local")

    val sc = new SparkContext(conf)

    //使用accumulator()方法创建自定义的Accumulator
    val sessionAggrStatAccumulator = sc.accumulator("")(SerssionAggrStatAccumulator)

    //模拟使用自定义的Accumulator
    var arr = Array(Constants.TIME_PERIOD_3m_10m, Constants.TIME_PERIOD_7s_9s, Constants.STEP_PERIOD_4_6)
    var rdd = sc.parallelize(arr, 1)
    rdd.foreach(sessionAggrStatAccumulator.add(_))

    println( " sessionAggrStatAccumulator.value = " + sessionAggrStatAccumulator.value)

  }

}
