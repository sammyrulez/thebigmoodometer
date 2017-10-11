package org.webdebs

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.io.Source

object SimpleApp {
  def main(args: Array[String]) {

    val conf: Config = ConfigFactory.load("application.conf")


    val stopWords =  Source.fromInputStream(getClass.getResourceAsStream(conf.getString("stopWordsFile"))).getLines().toList

    val spark = SparkSession.builder.appName("Simple Application")
     // .master("local[2]")
      .master(conf.getString("sparkUrl"))
      .config("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      .config("spark.jars", conf.getString("appClasses"))
      .getOrCreate()



    spark.stop()
  }
}
