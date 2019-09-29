package org.hrong.transformations

import org.hrong.utils.SparkProvider

/**
  * map针对每个元素：共调用10次
  * MapPartitions针对每个partition：共调用三次，因为在第12行调用的时候切片数为3
  */
object MapPartitions {
  def main(args: Array[String]): Unit = {
    val sc = SparkProvider.getSparkContext(appName = getClass.getSimpleName)
    val data = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)
    //使用累加器对map与MapPartitions的调用次数进行计数
    val mapAcc = sc.longAccumulator("map-accumulator")
    val mapPartitionsAcc = sc.longAccumulator("mapPartitions-accumulator")
    data.map(num => {
      mapAcc.add(1L)
      num + 1
    }).count()
    data.cache()
    println("map - accumulator:" + mapAcc.value)
    data.mapPartitions(item => {
      mapPartitionsAcc.add(1L)
      item.map(num => num + 1)
    }).count()
    println("mapPartitions-accumulator:" + mapPartitionsAcc.value)


  }
}
