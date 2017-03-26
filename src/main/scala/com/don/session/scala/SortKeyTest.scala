package com.don.session.scala

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Cao Wei Dong on 2017-03-26.
  */
object SortKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SortKeyTest")
      .setMaster("local")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    val ar = Array(
      Tuple2(new SortKey(30, 35, 40), "1"),
      Tuple2(new SortKey(35, 30, 40), "3"),
      Tuple2(new SortKey(30, 38, 30), "4")
    )
    val originalRDD = sc.parallelize(ar, 1)
    val sortedRdd = originalRDD.sortByKey(false)

    for(tuple <- sortedRdd.collect()) {
      println(tuple._2)
    }
  }
}
