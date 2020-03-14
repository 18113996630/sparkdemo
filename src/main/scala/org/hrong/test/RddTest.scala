package org.hrong.test

import org.apache.spark.{SparkConf, SparkContext}

object RddTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    val rdd1 = sc.parallelize(Seq(("a", 2), ("a", 5), ("a", 4), ("b", 5), ("c", 3), ("b", 3), ("c", 6), ("a", 8)), 3)


    // aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U)
    // seqOp为单个partition内的聚合操作：an operator used to accumulate results within a partition
    // combOp为不同partition之间聚合结果的聚合逻辑：an associative operator used to combine results from different partitions
    val r1 = rdd1.aggregate((0, 0))(
      (u, c) => {
        // 单个partition内的聚合操作
        // u在单个partition内的值为agg函数的初始值，即为：(0, 0)， c为当前partition内的元素，例如，第一个c值为：("a", 2)
        // u._1 + 1 则表示partition内存在一个值，则将初始值的第一位加1（求tuple的数量），u._2+c._2表示初始值的第二位与partition内的每个tuple的第二个值相加（对tuple的第二个值求和）
        (u._1 + 1, u._2 + c._2)
      },
      // 不同partition之间聚合结果的聚合逻辑：(tuple数量相加, 不同partition的tuple第二个值的聚合值相加)
      (r1, r2) => (r1._1 + r2._1, r1._2 + r2._2)
    )
    println(r1)

    val rdd = sc.parallelize(1 to 9, 3)
    val result = rdd.aggregate(0)(_ + _, _ + _)

    println(result)


    sc.stop()
  }

}
