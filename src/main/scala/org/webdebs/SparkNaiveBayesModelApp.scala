package org.webdebs

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.webdebs.spark.SparkUtils

import scala.io.Source

object SparkNaiveBayesModelApp extends App{

  val conf: Config = ConfigFactory.load("application.conf")


  def loadStopWords(stopWordsFileName: String): List[String] = {
    Source.fromInputStream(getClass.getResourceAsStream( stopWordsFileName)).getLines().toList
  }



    val spark = SparkSession.builder.appName("Simple Application")
      // .master("local[2]")
      .master(conf.getString("sparkUrl"))
      .config("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      .config("spark.jars", conf.getString("appClasses"))
      .getOrCreate()

    val sc = spark.sparkContext



    val stopWordsList = sc.broadcast(loadStopWords(conf.getString("stopWordsFile")))
    createAndSaveNBModel(sc, stopWordsList)
    validateAccuracyOfNBModel(sc, stopWordsList)


//lib
  def replaceNewLines(tweetText: String): String = {
    tweetText.replaceAll("\n", "")
  }



  def createAndSaveNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
    val tweetsDF: DataFrame = loadSentimentFile(sc, conf.getString("sentimentFile"))



    val labeledRDD = tweetsDF.select("polarity", "body").rdd.map {
      case Row(polarity: Int, body: String) =>
        val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(body, stopWordsList.value)
        LabeledPoint(polarity, MLlibSentimentAnalyzer.transformFeatures(tweetInWords))
    }
    labeledRDD.cache()

    val naiveBayesModel: NaiveBayesModel = NaiveBayes.train(labeledRDD, lambda = 1.0, modelType = "multinomial")
    naiveBayesModel.save(sc, conf.getString("NBFile"))
  }

 //MOVE
  def validateAccuracyOfNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
    val naiveBayesModel: NaiveBayesModel = NaiveBayesModel.load(sc, conf.getString("NBFile"))

    val tweetsDF: DataFrame = loadSentimentFile(sc, conf.getString("sentimentFile"))
    val actualVsPredictionRDD = tweetsDF.select("polarity", "body").rdd.map {
      case Row(polarity: Int, tweet: String) =>
        val tweetText = replaceNewLines(tweet)
        val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(tweetText, stopWordsList.value)
        (polarity.toDouble,
          naiveBayesModel.predict(MLlibSentimentAnalyzer.transformFeatures(tweetInWords)),
          tweetText)
    }
    val accuracy = 100.0 * actualVsPredictionRDD.filter(x => x._1 == x._2).count() / tweetsDF.count()

    println(f"""\n\t<==******** Prediction accuracy compared to actual: $accuracy%.2f%% ********==>\n""")
    SparkUtils.saveAccuracy(sc, actualVsPredictionRDD)
  }


  def loadSentimentFile(sc: SparkContext, sentimentFilePath: String): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val tweetsDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(sentimentFilePath)

       tweetsDF.toDF("subreddit","author", "body", "polarity")


  }







}
