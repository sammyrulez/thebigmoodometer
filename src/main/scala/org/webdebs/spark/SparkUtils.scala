package org.webdebs.spark

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.webdebs.SparkNaiveBayesModelApp.{conf, getClass}
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object SparkUtils {


  def saveAccuracy(sc: SparkContext, actualVsPredictionRDD: RDD[(Double, Double, String)]): Unit = {
    val sqlContext = buildSqlContext(sc)
    import sqlContext.implicits._
    val actualVsPredictionDF = actualVsPredictionRDD.toDF("Actual", "Predicted", "Text")
    actualVsPredictionDF.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      // Compression codec to compress while saving to file.
      .option("codec", classOf[GzipCodec].getCanonicalName)
      .mode(SaveMode.Append)
      .save(conf.getString("accuracyFile"))
  }

  def replaceNewLines(tweetText: String): String = {
    tweetText.replaceAll("\n", "")
  }

  def getBarebonesTweetText(tweetText: String, stopWordsList: List[String]): Seq[String] = {

    tweetText.toLowerCase()
      .replaceAll("\n", "")
      .replaceAll("rt\\s+", "")
      .replaceAll("\\s+@\\w+", "")
      .replaceAll("@\\w+", "")
      .replaceAll("\\s+#\\w+", "")
      .replaceAll("#\\w+", "")
      .replaceAll("(?:https?|http?)://[\\w/%.-]+", "")
      .replaceAll("(?:https?|http?)://[\\w/%.-]+\\s+", "")
      .replaceAll("(?:https?|http?)//[\\w/%.-]+\\s+", "")
      .replaceAll("(?:https?|http?)//[\\w/%.-]+", "")
      .split("\\W+")
      .filter(_.matches("^[a-zA-Z]+$"))
      .filter(!stopWordsList.contains(_))
  }

  def buildSqlContext(sc: SparkContext): SQLContext = {
   new org.apache.spark.sql.SQLContext(sc)
  }

  def loadStopWords(stopWordsFileName: String): List[String] = {
    Source.fromInputStream(getClass.getResourceAsStream( stopWordsFileName)).getLines().toList
  }
}
