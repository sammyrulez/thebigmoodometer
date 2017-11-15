package org.webdebs

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.webdebs.spark.SparkUtils

object MLlibSentimentAnalyzer {


  def computeSentiment(text: String, stopWordsList: Broadcast[List[String]], model: NaiveBayesModel): Int = {
    val tweetInWords: Seq[String] = SparkUtils.getBarebonesTweetText(text, stopWordsList.value)
    model.predict(MLlibSentimentAnalyzer.transformFeatures(tweetInWords)).toInt
  }

  val hashingTF = new HashingTF()


  def transformFeatures(tweetText: Seq[String]): Vector = {
    hashingTF.transform(tweetText)
  }
}
