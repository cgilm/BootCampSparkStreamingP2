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

  case class TransCount(status: String)

    /*
   * This stream handles the one hour running totals along with per minute aggregates
   */
  kafkaStream.window(Minutes(1), Seconds(60))
    .foreachRDD {
      (message: RDD[(String, String)], batchTime: Time) => {
        val df = message.map {
          case (k, v) => v.split(";")
        }.map(payload => {
          val initStatus = payload(9).toInt
          val status = if (initStatus < 5) s"REJECTED" else s"APPROVED"

          TransCount(status)
        }).toDF("status")

        val timeInMillis = System.currentTimeMillis()

        val currCal = new GregorianCalendar()
        currCal.setTime(new Timestamp(timeInMillis))

        val year = currCal.get(Calendar.YEAR)
        val month = currCal.get(Calendar.MONTH)
        val day = currCal.get(Calendar.DAY_OF_MONTH)
        val hour = currCal.get(Calendar.HOUR)
        val min = currCal.get(Calendar.MINUTE)

        val previnCal = new GregorianCalendar()
        prevCal.setTime(new Timestamp(timeInMillis))
        prevCal.add(Calendar.MINUTE, -1)

        val prevYear = prevCal.get(Calendar.YEAR)
        val prevMonth = prevCal.get(Calendar.MONTH)
        val prevDay = prevCal.get(Calendar.DAY_OF_MONTH)
        val prevHour = prevCal.get(Calendar.HOUR)
        val prevMin = prevCal.get(Calendar.MINUTE)
        
        // TODO: See readme for instructions
      }
    }



  ssc.start()
  ssc.awaitTermination()
}
