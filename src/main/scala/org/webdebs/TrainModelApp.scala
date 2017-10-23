package org.webdebs

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.webdebs.nlp.SentimentAnalyzer
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}

import scala.io.Source

case class RedditItem(body:String)


object TrainModelApp  extends App{

  //Read Data



  val sentimentParser = new SentimentAnalyzer

  val conf: Config = ConfigFactory.load("application.conf")

  val spark = SparkSession.builder.appName("Simple Application")
    .master("local[2]")
    .master(conf.getString("sparkUrl"))
    .config("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    .config("spark.jars", conf.getString("appClasses"))
    .getOrCreate()

  import spark.implicits._




  val dsItems: DataFrame = spark.read.textFile("/Users/sam/Downloads/2017/subset.2017.json")
    .map(record => {
     "happy"
    })
    .map(str => (str, 1)).toDF("text","polarity")




  val labeledRDD = dsItems.select("polarity", "text").rdd.map {
    case Row(polarity: Int, text: String) =>
      val tweetInWords: Seq[String] = text.split(" ").toSeq //MLlibSentimentAnalyzer.getBarebonesTweetText(tweet, stopWordsList.value)
      LabeledPoint(polarity, transformFeatures(tweetInWords))
  }
  labeledRDD.cache()
  labeledRDD.foreach(println)

  val naiveBayesModel: NaiveBayesModel = NaiveBayes.train(labeledRDD, lambda = 1.0, modelType = "multinomial")
  naiveBayesModel.save(spark.sparkContext, conf.getString("naiveBayesModelPath"))




  def transformFeatures(redditText: Seq[String]): Vector = {
    val hashingTF = new HashingTF
    hashingTF.transform(redditText)
  }

 // new LabeledPoint(sentimentParser.computeSentiment(str), transformFeatures(str.split(" ").toSeq))


  dsItems
    .take(30).foreach(println)




  //Train

}
