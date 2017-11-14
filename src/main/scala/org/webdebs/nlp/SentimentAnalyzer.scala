package org.webdebs.nlp

import java.lang.Exception
import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class SentimentAnalyzer {

  lazy val pipeline = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    new StanfordCoreNLP(props)
  }

  def computeSentiment(text: String): Int = {
    try {
    val (_, sentiment) = extractSentiments(text)
      .maxBy { case (sentence, _) => sentence.length }
    sentiment
    }
    catch {
      case e:Exception => 0
    }
  }

  def extractSentiments(text: String): List[(String, Int)] = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString, normalizeSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
      .toList
  }

  def normalizeSentiment(sentiment: Double): Int = {
    sentiment match {
      case s if s <= 0.0 => 0 // neutral
      case s if s < 2.0 => -1 // negative
      case s if s < 3.0 => 0 // neutral
      case s if s < 5.0 => 1 // positive
      case _ => 0 // neutral.
    }
  }





}
