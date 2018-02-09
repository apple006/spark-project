package com.kongbig.sparkproject.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Describe: 
  * Author:   kongbig
  * Data:     2018/2/9.
  */
object SortKeyTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortKeyTest").setMaster("local")
    val sc = new SparkContext(conf)

    val arr = Array(Tuple2(new SortKey(30, 35, 40), "1"),
      Tuple2(new SortKey(35, 35, 40), "2"),
      Tuple2(new SortKey(30, 38, 30), "3"))
    val rdd = sc.parallelize(arr, 1)

    val sortedRdd = rdd.sortByKey(false)

    for (tuple <- sortedRdd.collect()) {
      println(tuple._2)
    }
  }

}
