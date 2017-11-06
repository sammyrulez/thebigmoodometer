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

import scala.io.Source

object SparkNaiveBayesModelApp {

  val conf: Config = ConfigFactory.load("application.conf")


  def loadStopWords(stopWordsFileName: String): List[String] = {
    Source.fromInputStream(getClass.getResourceAsStream( stopWordsFileName)).getLines().toList
  }

  def main(args: Array[String]) {

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
  }

  /**
    * Remove new line characters.
    *
    * @param tweetText -- Complete text of a tweet.
    * @return String with new lines removed.
    */
  def replaceNewLines(tweetText: String): String = {
    tweetText.replaceAll("\n", "")
  }


  /**
    * Creates a Naive Bayes Model of Tweet and its Sentiment from the Sentiment140 file.
    *
    * @param sc            -- Spark Context.
    * @param stopWordsList -- Broadcast variable for list of stop words to be removed from the tweets.
    */
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

  /**
    * Validates and check the accuracy of the model by comparing the polarity of a tweet from the dataset and compares it with the MLlib predicted polarity.
    *
    * @param sc            -- Spark Context.
    * @param stopWordsList -- Broadcast variable for list of stop words to be removed from the tweets.
    */
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
    /*actualVsPredictionRDD.cache()
    val predictedCorrect = actualVsPredictionRDD.filter(x => x._1 == x._2).count()
    val predictedInCorrect = actualVsPredictionRDD.filter(x => x._1 != x._2).count()
    val accuracy = 100.0 * predictedCorrect.toDouble / (predictedCorrect + predictedInCorrect).toDouble*/
    println(f"""\n\t<==******** Prediction accuracy compared to actual: $accuracy%.2f%% ********==>\n""")
    saveAccuracy(sc, actualVsPredictionRDD)
  }

  /**
    * Loads the Sentiment140 file from the specified path using SparkContext.
    *
    * @param sc                   -- Spark Context.
    * @param sentimentFilePath -- Absolute file path of Sentiment140.
    * @return -- Spark DataFrame of the Sentiment file with the tweet text and its polarity.
    */
  def loadSentimentFile(sc: SparkContext, sentimentFilePath: String): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val tweetsDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(sentimentFilePath)

       tweetsDF.toDF("subreddit","author", "body", "polarity")


  }

  /**
    * Saves the accuracy computation of the ML library.
    * The columns are actual polarity as per the dataset, computed polarity with MLlib and the tweet text.
    *
    * @param sc                    -- Spark Context.
    * @param actualVsPredictionRDD -- RDD of polarity of a tweet in dataset and MLlib computed polarity.
    */
  def saveAccuracy(sc: SparkContext, actualVsPredictionRDD: RDD[(Double, Double, String)]): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
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




}
