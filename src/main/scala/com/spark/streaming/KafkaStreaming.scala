
package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object KafkaStreaming extends App {

  val conf = new org.apache.spark.SparkConf().setMaster("local[*]").setAppName("kafka-streaming")
  val context = new SparkContext(conf)
  val ssc = new StreamingContext(context, org.apache.spark.streaming.Seconds(10))
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean))
  val topics = Array("one")
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams))

  val content = stream.filter(x => x.value() != null)
  val sqlContext = new org.apache.spark.sql.SQLContext(context)
      import sqlContext.implicits._

  stream.map(_.value).foreachRDD(rdd => {

    rdd.foreach(println)
    if (!rdd.isEmpty()) {
      rdd.toDF("value").coalesce(1).write.mode(SaveMode.Append).json("C:/data/spark/")
     // rdd.saveAsTextFile("C:/data/spark/")
    }

  })
  ssc.start()
  ssc.awaitTermination()
}