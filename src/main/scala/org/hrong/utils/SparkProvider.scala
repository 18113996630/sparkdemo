package org.hrong.utils

import org.apache.spark.{SparkConf, SparkContext}

object SparkProvider {
  def getSparkConf(master:String = "local[4]", appName:String):SparkConf = {
    new SparkConf().setMaster(master).setAppName(appName)
  }

  def getSparkContext(master:String = "local[4]", appName:String):SparkContext = {
    new SparkContext(master, appName)
  }
}
