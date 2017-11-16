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






    val spark = SparkSession.builder.appName("Simple Application")
      // .master("local[2]")
      .master(conf.getString("sparkUrl"))
      .config("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      .config("spark.jars", conf.getString("appClasses"))
      .getOrCreate()

    val sc = spark.sparkContext



    val stopWordsList = sc.broadcast(SparkUtils.loadStopWords(conf.getString("stopWordsFile")))
    createAndSaveNBModel(sc, stopWordsList)
    validateAccuracyOfNBModel(sc, stopWordsList)

  def loadSentimentFile(sc: SparkContext, sentimentFilePath: String): DataFrame = {
    val sqlContext = SparkUtils.buildSqlContext(sc)
    val postsDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(sentimentFilePath)

    postsDF.toDF("subreddit","author", "body", "polarity")

  }


  def createAndSaveNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
    val postsDF: DataFrame = loadSentimentFile(sc, conf.getString("sentimentFile"))

    val labeledRDD = postsDF.select("polarity", "body").rdd.map {
      case Row(polarity: Int, body: String) =>
        val tweetInWords: Seq[String] = SparkUtils.getBarebonesTweetText(body, stopWordsList.value)
        LabeledPoint(polarity, MLlibSentimentAnalyzer.transformFeatures(tweetInWords))
    }
    labeledRDD.cache()

    val naiveBayesModel: NaiveBayesModel = NaiveBayes.train(labeledRDD, lambda = 1.0, modelType = "multinomial")
    naiveBayesModel.save(sc, conf.getString("NBFile"))
  }


  def validateAccuracyOfNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
    val naiveBayesModel: NaiveBayesModel = NaiveBayesModel.load(sc, conf.getString("NBFile"))

    val postsDF: DataFrame = loadSentimentFile(sc, conf.getString("sentimentFile"))
    val actualVsPredictionRDD = postsDF.select("polarity", "body").rdd.map {
      case Row(polarity: Int, tweet: String) =>
        val tweetText = SparkUtils.replaceNewLines(tweet)
        val tweetInWords: Seq[String] = SparkUtils.getBarebonesTweetText(tweetText, stopWordsList.value)
        (polarity.toDouble,
          naiveBayesModel.predict(MLlibSentimentAnalyzer.transformFeatures(tweetInWords)),
          tweetText)
    }
    val accuracy = 100.0 * actualVsPredictionRDD.filter(x => x._1 == x._2).count() / postsDF.count()

    println(
      f"""*******************************
         |\n\tAccuracy: $accuracy%.2f%% \n
         |*******************************""".stripMargin)
    SparkUtils.saveAccuracy(sc, actualVsPredictionRDD)
  }










}
