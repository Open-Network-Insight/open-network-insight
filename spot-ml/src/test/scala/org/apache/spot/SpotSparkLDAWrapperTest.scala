package org.apache.spot

import breeze.linalg.DenseMatrix
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spot.SpotLDAWrapper.SpotLDAInput
import org.apache.spot.SpotSparkLDAWrapper.{formatSparkLDADocTopicOutput, formatSparkLDAInput, formatSparkLDAWordOutput}
import org.apache.spot.testutils.TestingSparkContextFlatSpec
import org.scalatest.Matchers

import scala.collection.Map


class SpotSparkLDAWrapperTest extends TestingSparkContextFlatSpec with Matchers {



  "SparkLDA" should "handle a two word doc" in {
    val logger = LogManager.getLogger("SuspiciousConnectsAnalysis")
    logger.setLevel(Level.INFO)

    val catFancy= SpotLDAInput("pets", "cat", 1)
    val dogWorld = SpotLDAInput("pets", "dog", 999)

    val data = sparkContext.parallelize(Seq(catFancy, dogWorld))
    val out = SpotSparkLDAWrapper.runLDA(data, 2, logger, Some(0xdeadbeef), "em")

    val topicMix =  out .docToTopicMix("pets")
    val catTopics = out.wordResults("cat")
    val dogTopics = out.wordResults("dog")

    Math.abs(topicMix(0)*catTopics(0) + topicMix(1)*catTopics(1)) should be < 0.01
    Math.abs(0.999 - (topicMix(0)*dogTopics(0) + topicMix(1)*dogTopics(1))) should be < 0.01
  }

  "SparkLDA" should "handle distinct docs on distinct words" in {
    val logger = LogManager.getLogger("SuspiciousConnectsAnalysis")
    logger.setLevel(Level.INFO)

    val catFancy= SpotLDAInput("cat fancy", "cat", 1)
    val dogWorld = SpotLDAInput("dog world", "dog", 1)

    val data = sparkContext.parallelize(Seq(catFancy, dogWorld))
    val out = SpotSparkLDAWrapper.runLDA(data,2, logger, None, "em")

    val dogTopicMix =  out .docToTopicMix("dog world")
    val catTopicMix =  out .docToTopicMix("cat fancy")

    val catTopics = out.wordResults("cat")
    val dogTopics = out.wordResults("dog")

    Math.abs(1 - (catTopicMix(0)*catTopics(0) + catTopicMix(1)*catTopics(1))) should be < 0.01
    Math.abs(1 - (dogTopicMix(0)*dogTopics(0) + dogTopicMix(1)*dogTopics(1))) should be < 0.01
  }


  "formatSparkLDAInput" should "return input in RDD[(Long, Vector)] (collected as Array for testing) format. The index is the docID, values are the vectors of word occurrences in that doc" in {

    val documentWordData = sparkContext.parallelize(Array(SpotLDAInput("192.168.1.1", "333333_7.0_0.0_1.0", 8),
      SpotLDAInput("10.10.98.123", "1111111_6.0_3.0_5.0", 4),
      SpotLDAInput("66.23.45.11", "-1_43_7.0_2.0_6.0", 2),
      SpotLDAInput("192.168.1.1", "-1_80_6.0_1.0_1.0", 5)))

    val wordDictionary = Map("333333_7.0_0.0_1.0" -> 0, "1111111_6.0_3.0_5.0" -> 1, "-1_43_7.0_2.0_6.0" -> 2, "-1_80_6.0_1.0_1.0" -> 3)

    val distinctDocument = documentWordData.map({ case SpotLDAInput(doc, word, count) => doc }).distinct.collect()

    val documentDictionary: Map[Int, String] = {
      distinctDocument
        .zipWithIndex
        .sortBy(_._2)
        .map(kv => (kv._2, kv._1))
        .toMap
    }
    val docStrToID: Map[String, Int] = documentDictionary.map(_.swap)

    val sparkLDAInput: RDD[(Long, Vector)] = SpotSparkLDAWrapper.formatSparkLDAInput(documentWordData, docStrToID, wordDictionary)
    val sparkLDAInArr: Array[(Long, Vector)] = sparkLDAInput.collect()

    sparkLDAInArr shouldBe Array((0,Vectors.sparse(4, Array(0,3), Array(8.0,5.0))), (1,Vectors.sparse(4,Array(1),Array(4.0))), (2,Vectors.sparse(4,Array(2),Array(2.0))))
  }

  "formatSparkLDADocTopicOuptut" should "return Map[Int,String] after converting doc results from vector: convert docID back to string, convert vector of probabilities to array" in {

    val documentWordData = sparkContext.parallelize(Array(SpotLDAInput("192.168.1.1", "333333_7.0_0.0_1.0", 8),
      SpotLDAInput("10.10.98.123", "1111111_6.0_3.0_5.0", 4),
      SpotLDAInput("66.23.45.11", "-1_43_7.0_2.0_6.0", 2),
      SpotLDAInput("192.168.1.1", "-1_80_6.0_1.0_1.0", 5)))

    val wordDictionary = Map("333333_7.0_0.0_1.0" -> 0, "1111111_6.0_3.0_5.0" -> 1, "-1_43_7.0_2.0_6.0" -> 2, "-1_80_6.0_1.0_1.0" -> 3)

    val distinctDocument = documentWordData.map({ case SpotLDAInput(doc, word, count) => doc }).distinct.collect()

    val documentDictionary: Map[Int, String] = {
      distinctDocument
        .zipWithIndex
        .sortBy(_._2)
        .map(kv => (kv._2, kv._1))
        .toMap
    }

    val docTopicDist: RDD[(Long, Vector)] = sparkContext.parallelize(Array((0.toLong, Vectors.dense(0.15, 0.3, 0.5, 0.05)), (1.toLong,
      Vectors.dense(0.25, 0.15, 0.4, 0.2)), (2.toLong, Vectors.dense(0.4, 0.1, 0.3, 0.2))))

    val sparkDocRes = formatSparkLDADocTopicOutput(docTopicDist, documentDictionary)

    sparkDocRes should contain key ("192.168.1.1")
    sparkDocRes should contain key ("10.10.98.123")
    sparkDocRes should contain key ("66.23.45.11")
  }

  "formatSparkLDAWordOutput" should "return Map[Int,String] after converting word matrix to sequence, wordIDs back to strings, and sequence of probabilities to array" in {
    val testMat = Matrices.dense(4, 4, Array(0.5, 0.2, 0.05, 0.25, 0.25, 0.1, 0.15, 0.5, 0.1, 0.4, 0.25, 0.25, 0.7, 0.2, 0.02, 0.08))

    val wordDictionary = Map("-1_23.0_7.0_7.0_4.0" -> 3, "23.0_7.0_7.0_4.0" -> 0, "333333.0_7.0_7.0_4.0" -> 2, "80.0_7.0_7.0_4.0" -> 1)
    val revWordMap: Map[Int, String] = wordDictionary.map(_.swap)

    val sparkWordRes = formatSparkLDAWordOutput(testMat, revWordMap)

    sparkWordRes should contain key ("23.0_7.0_7.0_4.0")
    sparkWordRes should contain key ("80.0_7.0_7.0_4.0")
    sparkWordRes should contain key ("333333.0_7.0_7.0_4.0")
    sparkWordRes should contain key ("-1_23.0_7.0_7.0_4.0")
  }

}