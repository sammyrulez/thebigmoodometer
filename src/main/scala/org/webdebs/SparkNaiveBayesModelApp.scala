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




    val stopWordsList = sc.broadcast(SparkUtils.loadStopWords(conf.getString("stopWordsFile"))) //"subreddit","author", "body", "polarity"




  train(sc,stopWordsList)


  def train(sc:SparkContext,stopWords:Broadcast[List[String]]):Unit = {


    val posts = load(sc,conf.getString("sentimentFile"))
    val labeledPoint = posts.select("polarity","body").rdd.collect {
      case Row(polarity:Int,body:String) => {
        val words = SparkUtils.getBarebonesText(body,stopWords.value)
        LabeledPoint(polarity,MLlibSentimentAnalyzer.transformFeatures(words))
      }
    }

    labeledPoint.cache()

    val naiveBayes = NaiveBayes.train(labeledPoint)
    naiveBayes.save(sc,conf.getString("NBFile"))

  }




  def load(sc:SparkContext,path:String):DataFrame = {
    val sqlContext = SparkUtils.buildSqlContext(sc)
    val posts = sqlContext.read
        .format("com.databricks.spark.csv")
         .option("header","false")
           .option("inferSchema","true")
            .option("escape","\"")
            .option("quote","\"")
            .load(path)
    posts.toDF("subreddit","author","body","polarity")
  }


  def createAndSaveNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
    val postsDF: DataFrame = loadSentimentFile(sc, conf.getString("sentimentFile"))

    val labeledRDD = postsDF.select("polarity", "body").rdd.collect {
      case Row(polarity: Int, body: Any) =>
        val tweetInWords: Seq[String] = SparkUtils.getBarebonesText(body.toString, stopWordsList.value)

        LabeledPoint(polarity, MLlibSentimentAnalyzer.transformFeatures(tweetInWords))
    }
    labeledRDD.cache()
    val effectiveLabels = labeledRDD.count();
    println(
      f"""
        |labels : $effectiveLabels
      """.stripMargin)

    val naiveBayesModel: NaiveBayesModel = NaiveBayes.train(labeledRDD, lambda = 1.0, modelType = "multinomial")
    naiveBayesModel.save(sc, conf.getString("NBFile"))
  }




  def loadSentimentFile(sc: SparkContext, sentimentFilePath: String): DataFrame = {
    val sqlContext = SparkUtils.buildSqlContext(sc)
    val postsDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("quote","\"")
      .option("escape","\"")
      .load(sentimentFilePath)

    postsDF.toDF("subreddit","author", "body", "polarity")

  }

  def validateAccuracyOfNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
    val naiveBayesModel: NaiveBayesModel = NaiveBayesModel.load(sc, conf.getString("NBFile"))

    val postsDF: DataFrame = loadSentimentFile(sc, conf.getString("sentimentTestFile"))
    val actualVsPredictionRDD = postsDF.select("polarity", "body").rdd.collect {
      case Row(polarity: Int, post: String) =>

        val tweetInWords: Seq[String] = SparkUtils.getBarebonesText(post, stopWordsList.value)
        (polarity,
          naiveBayesModel.predict(MLlibSentimentAnalyzer.transformFeatures(tweetInWords)).toInt,
          post)
    }
    val accuracy = 100.0 * actualVsPredictionRDD.filter(x => x._1 == x._2).count() / postsDF.count()

    println(
      f"""*******************************
         |\n\tAccuracy: $accuracy%.2f%% \n
         |*******************************""".stripMargin)
    SparkUtils.saveAccuracy(sc, actualVsPredictionRDD)
  }










}
