package org.webdebs

import java.io.{File, PrintWriter}

import com.typesafe.config.{Config, ConfigFactory}
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.commons.lang.StringEscapeUtils
import org.webdebs.nlp.SentimentAnalyzer

import scala.io.Source


case class RedditItem(body: String, subreddit: String, author: String)

object NormalizeApp extends App {

  val sentiment = new SentimentAnalyzer

  val conf: Config = ConfigFactory.load("application.conf")

  val writer = new PrintWriter(new File(conf.getString("sentimentFile")))


  Source.fromFile(conf.getString("rawData"))
    .getLines()
    .map(src => decode[RedditItem](src))
    .collect {
      case Right(i) => i
    }.map(ri => List(ri.subreddit, ri.author, StringEscapeUtils.escapeCsv(ri.body.replace('\n', ' ')), sentiment.computeSentiment(ri.body)))
     .map(l => l.mkString(",")+"\n")
    .foreach(writer.write)
   writer.close()






}
