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


case class RedditItem(body:String,subreddit:String,author:String)

object NormalizeApp extends App{

 val sentiment = new SentimentAnalyzer

  val conf: Config = ConfigFactory.load("application.conf")

  val writer = new PrintWriter(new File(conf.getString("sentimentFile")))


  Source.fromFile(conf.getString("redditDumpPath")).getLines().map(
    str => decode[RedditItem](str)
  ).collect{
    case Right(item) => item
  }.map(item => List(item.subreddit, item.author,StringEscapeUtils.escapeCsv(item.body.replace('\n',' ')),sentiment.computeSentiment(item.body)))
    .map(l => l.mkString(",") + "\n")
    .foreach(writer.write)


  writer.close()



}
