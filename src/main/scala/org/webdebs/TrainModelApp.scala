package org.webdebs

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.webdebs.nlp.SentimentAnalyzer

import scala.io.Source


object TrainModelApp  extends App{

  //Read Data

  case class RedditItem(body:String)

  val sentimentParser = new SentimentAnalyzer

  Source.fromFile("/Users/sam/Downloads/2017/RC_2017-01").getLines()
    .map(str => decode[RedditItem](str) )
    .filter(_.isRight)
    .map(_.right.get.body)
    .map(str => (str,sentimentParser.computeSentiment(str)))
    .take(30).foreach(println)


  //Train

}
