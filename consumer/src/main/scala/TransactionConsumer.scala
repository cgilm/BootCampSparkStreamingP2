package com.datastax.demo.fraudprevention
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
  * Created by carybourgeois on 10/30/15.
  * Modified by cgilmore on 5/20/16
  */

import java.util.{GregorianCalendar, Calendar}
import java.text.SimpleDateFormat
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.{Minutes, Seconds, Milliseconds, StreamingContext, Time}
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import java.sql.Timestamp
import org.apache.spark.sql.functions._

object TransactionConsumer extends App {

  // Get configuration properties
  val systemConfig = ConfigFactory.load()
  val appName = systemConfig.getString("TransactionConsumer.sparkAppName")
  val kafkaHost = systemConfig.getString("TransactionConsumer.kafkaHost")
  val kafkaDataTopic = systemConfig.getString("TransactionConsumer.kafkaDataTopic")
  val dseKeyspace = systemConfig.getString("TransactionConsumer.dseKeyspace")
  val dseTable = systemConfig.getString("TransactionConsumer.dseTable")

 // configure the number of cores and RAM to use
  val conf = new SparkConf()
    .set("spark.cores.max", "4")
    .set("spark.executor.memory", "2G")
    .setAppName(appName)
    
  val sc = SparkContext.getOrCreate(conf)
  val sqlContext = SQLContext.getOrCreate(sc)
  val ssc = new StreamingContext(sc, Seconds(1))
  ssc.checkpoint(appName)
  import sqlContext.implicits._

  // configure kafka connection and topic
  val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaHost)
  val kafkaTopics = Set(kafkaDataTopic)
  val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics)

  case class Transaction(cc_no:String,
                         cc_provider: String,
                         year: Int,
                         month: Int,
                         day: Int,
                         hour: Int,
                         min: Int,
                         txn_time: Timestamp,
                         txn_id: String,
                         merchant: String,
                         location: String,
                         country: String,
                         items: Map[String, Double],
                         amount: Double,
                         status: String,
                         date_text: String)

  case class TransCount(status: String)

  /*
   * Stream transactions to Cassandra, flag any transactions that have an initstatus < 5 as REJECTED
   */
  kafkaStream.window(Seconds(1), Seconds(1))
    .foreachRDD {
      (message: RDD[(String, String)], batchTime: Time) => {
        val df = message.map {
          case (k, v) => v.split(";")
        }.map(payload => {
         
          val cc_no = payload(0)
          val cc_provider = payload(1)
          val txn_time = Timestamp.valueOf(payload(2))
          val calendar = new GregorianCalendar()
          calendar.setTime(txn_time)

          val txn_id = payload(3)
          val merchant = payload(4)
          val location = payload(5)
          val country = payload(6)
          val items = payload(7).split(",").map(_.split("->")).map { case Array(k, v) => (k, v.toDouble) }.toMap
          val amount = payload(8).toDouble

          // Simple use of status to set REJECTED or APPROVED
          val initStatus = payload(9).toInt
          
          val dateFormat = new SimpleDateFormat("yyyymmdd")
          val date_text = dateFormat.format(calendar.getTime())
          
          
          //TODO : See readme for what to write here

      }
    }



  ssc.start()
  ssc.awaitTermination()
}
