package com.example

import java.io.FileWriter

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD

object StreamingApp {

  def main(args:Array[String]) {
    val conf = new SparkConf()
      .setAppName("StreamingApp")
      .setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "200")

    val ssc = new StreamingContext(conf, Seconds(5))
    val sc = ssc.sparkContext

    val preferredHosts = LocationStrategies.PreferConsistent
    val topics = List("kafka-to-spark-streaming")

    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming",
      "auto.offset.reset" -> "earliest"
    )

    import org.apache.kafka.common.TopicPartition
    val offsets = Map(new TopicPartition("kafka-to-spark-streaming", 0) -> 2L)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets))

    var index = 1
    stream.foreachRDD { rdd =>
      // println(s"Rdd Count: ${rdd.count()}")

      process(rdd.map(_.value()), index, sc)
      index += 1

      // Get the offset ranges in the RDD
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} offsets: ${o.fromOffset} to ${o.untilOffset}")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def process(rdd: RDD[String], index: Int, sc: SparkContext) {

    rdd.cache()

    // map lines to Users
    val users = rdd
      .map(_.split(","))
      .map(User(_))

    // sum by data 5 overall
    val sum = users.map(_.data5).sum()

    // distinct users count
    val distinct = users.map(_.data1).distinct().count()

    // average by data 3 by user
    val average = users.keyBy(_.data1).aggregateByKey((0F, 0))(
      (acc, user) => (acc._1 + user.data3, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .mapValues(sum => sum._1 / sum._2)

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

    // join user data
    val userData = average.join(recentValues)
      .map { case (userId, (averageData, recentData)) =>
        List(userId, averageData.toString, "%.0f".format(recentData)).mkString(",") }

    // initial stats (sum data overall, distinct users)
    val statsData = sc.parallelize(List("%.0f".format(sum), distinct.toString))

    // save csv
    val csvData = statsData.union(userData)

    // in a cluster, we would write to HDFS instead
    // csvData.saveAsTextFile(s"data/${1}.csv")

    writeCsv(csvData.collect(), s"output/$index.csv")
  }

  def writeCsv(data: Array[String], path: String): Unit = {

    def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
      try { f(param) } finally { param.close() }

    using (new FileWriter(path)) {
      fileWriter => fileWriter.write(data.mkString("\n"))
    }
  }
}
