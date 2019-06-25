package com.example

import java.io.FileWriter

import org.apache.spark._

object BatchApp {

  def main(args:Array[String]) {
    val conf = new SparkConf()
      .setAppName("BatchApp")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // read lines from file
    val users = sc.textFile("transactions_1000.csv")
                  .map(_.split(","))
                  .map(User(_))

    // lines count
    println(s"Count: ${users.count()}")

    // sum by data 5 overall
    val sum = users.map(_.data5).sum()
    println(s"Sum: ${"%.0f".format(sum)}")

    // distinct users count
    val distinct = users.map(_.data1).distinct().count()
    println(s"Distinct users: $distinct")

    // average by data 3 by user
    val average = users.keyBy(_.data1).aggregateByKey((0F, 0))(
      (acc, user) => (acc._1 + user.data3, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .mapValues(sum => sum._1 / sum._2)

    average.take(10)
      .foreach(p => println(s"User: ${p._1}, Average: ${p._2}"))

    // most recent data 4 by user
    // add an index to each element, in order
    val rddWithIndex = users.zipWithIndex().keyBy(_._1.data1)

    val addToPartition = (acc: (Float, Long), value: (User, Long)) => {
      val mostRecentIndex = math.max(acc._2, value._2)
      val mostRecentValue = if (acc._2 == mostRecentIndex) acc._1 else value._1.data4

      (mostRecentValue, mostRecentIndex)
    }

    val mergePartitions = (acc1: (Float, Long), acc2: (Float, Long)) => {
      val mostRecentIndex = math.max(acc1._2, acc2._2)
      val mostRecentValue = if (acc1._2 == mostRecentIndex) acc1._1 else acc2._1

      (mostRecentValue, mostRecentIndex)
    }

    val recentValues = rddWithIndex.aggregateByKey((0F, 0L))(addToPartition, mergePartitions)
      .map { case (userId, (value, _)) => (userId, value) }

    recentValues.take(10)
      .foreach(p => println(s"User: ${p._1}, Most recent value: ${"%.0f".format(p._2)}"))

    // join user data
    val userData = average.join(recentValues)
      .map { case (userId, (averageData, recentData)) =>
        List(userId, averageData.toString, "%.0f".format(recentData)).mkString(",") }

    userData.take(10)
      .foreach(println(_))

    // initial stats (sum data overall, distinct users)
    val statsData = sc.parallelize(List("%.0f".format(sum), distinct.toString))

    // save csv
    val csvData = statsData.union(userData)

    // csvData.saveAsTextFile(s"data/${1}.csv")

    writeCsv(csvData.collect(), s"output/${1}.csv")
  }

  def writeCsv(data: Array[String], path: String): Unit = {

    def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
      try { f(param) } finally { param.close() }

    using (new FileWriter(path)) {
      fileWriter => fileWriter.write(data.mkString("\n"))
    }
  }
}
