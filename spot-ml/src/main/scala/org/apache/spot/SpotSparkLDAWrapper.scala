package org.apache.spot

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{DistributedLDAModel, EMLDAOptimizer, LDA, OnlineLDAOptimizer}
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spot.SpotLDAWrapper.{SpotLDAInput, SpotLDAOutput}
import org.apache.spot.spotldacwrapper.SpotLDACSchema._

import scala.collection.Map

/**
  * Spark LDA implementation
  * Contains routines for LDA using Scala Spark implementation from mllib
  * 1. Creates list of unique documents, words and model based on those two
  * 2. Processes the model using Spark LDA
  * 3. Reads Spark LDA results: Topic distributions per document (docTopicDist) and word distributions per topic (wordTopicMat)
  * 4. Convert wordTopicMat (Matrix) and docTopicDist (RDD) to appropriate formats (maps) originally specified in LDA-C
  */

object SpotSparkLDAWrapper {


  def runLDA(sparkContext: SparkContext,
             sqlContext: SQLContext,
             docWordCount: RDD[SpotLDAInput],
             topicCount: Int,
             logger: Logger,
             ldaSeed: Option[Long],
             ldaOptimizer: String = "em",
             ldaAlpha: Double = 1.02,
             ldaBeta: Double = 1.001,
             maxIterations: Int = 20): SpotLDAOutput = {

    // Create word Map Word,Index for further usage
    val wordDictionary: Map[String, Int] = {
      val words = docWordCount
        .cache
        .map({ case SpotLDAInput(doc, word, count) => word })
        .distinct
        .collect
      words.zipWithIndex.toMap
    }

    val distinctDocument: Array[String] = docWordCount.map({ case SpotLDAInput(doc, word, count) => doc }).distinct.collect

    // Create document Map Index, Document for further usage
    val documentDictionary: Map[Int, String] = {
      distinctDocument
        .zipWithIndex
        .sortBy(_._2)
        .map(kv => (kv._2, kv._1))
        .toMap
    }
    val docStrToID: Map[String, Int] = documentDictionary.map(_.swap)

    //Structure corpus so that the index is the docID, values are the vectors of word occurrences in that doc
    val ldaCorpus: RDD[(Long, Vector)] = formatSparkLDAInput(docWordCount, docStrToID, wordDictionary)

    //Instantiate optimizer based on input
    val optimizer = ldaOptimizer match {
      case "em" => new EMLDAOptimizer
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / distinctDocument.size)
      case _ => throw new IllegalArgumentException(s"Only em and online are supported but got $ldaOptimizer")
    }

    logger.info("Running Spark LDA with params alpha = " + ldaAlpha + " beta = " + ldaBeta + " Max iters = " + maxIterations + " Optimizer = " + ldaOptimizer)
    //Set LDA params from input args

    val lda =
      new LDA().setK(topicCount).setMaxIterations(maxIterations).setAlpha(ldaAlpha).setBeta(ldaBeta).setOptimizer(optimizer)

    //If seed not set, the automatically set to hash value of class name
    def unrollSeed(opt: Option[Long]): Long = opt getOrElse -1L
    val ldaSeedLong = unrollSeed(ldaSeed)
    if (ldaSeedLong != -1) lda.setSeed(ldaSeedLong)

    //Create LDA model
    val ldaModel = lda.run(ldaCorpus)

    //Convert to DistributedLDAModel to expose info about topic distribution
    val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]

    val avgLogLikelihood = distLDAModel.logLikelihood / distinctDocument.size
    logger.info("Spark LDA Log likelihood: " + avgLogLikelihood)

    //Get word topic mix: columns = topic (in no guaranteed order), rows = words (# rows = vocab size)
    val wordTopicMat: Matrix = distLDAModel.topicsMatrix

    //Topic distribution: for each document, return distribution (vector) over topics for that docs
    val docTopicDist: RDD[(Long, Vector)] = distLDAModel.topicDistributions

    //Create doc results from vector: convert docID back to string, convert vector of probabilities to array
    val docToTopicMix: RDD[(String, Array[Double])] = formatSparkLDADocTopicOutput(docTopicDist, documentDictionary)
    import sqlContext.implicits._

    val docToTopicMixDF = docToTopicMix.toDF(DocumentName, TopicProbabilityMix)
    //Create word results from matrix: convert matrix to sequence, wordIDs back to strings, sequence of probabilities to array
    val revWordMap: Map[Int, String] = wordDictionary.map(_.swap)

    val wordResults = formatSparkLDAWordOutput(wordTopicMat, revWordMap)

    //Create output object
    SpotLDAOutput(docToTopicMixDF, wordResults)
  }

  def formatSparkLDAInput(docWordCount: RDD[SpotLDAInput], docStrToID: Map[String, Int], wordDictionary: Map[String, Int]): RDD[(Long, Vector)] = {
    //Convert SpotSparkLDAInput into desired format for Spark LDA: (doc, word, count) -> word count per doc, where RDD
    //is indexed by DocID
    val wordCountsPerDoc: RDD[(Long, Iterable[(Int, Double)])]
    = docWordCount.map({ case SpotLDAInput(doc, word, count) => (docStrToID(doc).toLong, (wordDictionary(word), count.toDouble)) }).groupByKey

    //Sum of distinct words in each doc (words will be repeated between different docs), used for sparse vec size
    val numUniqWords = wordDictionary.size
    val ldaInput: RDD[(Long, Vector)] = wordCountsPerDoc.mapValues({ case vs => Vectors.sparse(numUniqWords, vs.toSeq) })

    ldaInput
  }


  def formatSparkLDAWordOutput(wordTopMat: Matrix, wordMap: Map[Int, String]): scala.Predef.Map[String, Array[Double]] = {


    // incoming word top matrix is in column-major order and the columns are unnormalized
    val array = wordTopMat.toArray
    val m = wordTopMat.numRows
    val n = wordTopMat.numCols
    val columnSums: Array[Double] = Range(0, n).map(j => (Range(0, m).map(i => wordTopMat(i, j)).sum)).toArray

    val wordProbs: Seq[Array[Double]] = wordTopMat.transpose.toArray.grouped(n).toSeq
      .map(unnormProbs => unnormProbs.zipWithIndex.map({ case (u, j) => u / columnSums(j) }))

    wordProbs.zipWithIndex.map({ case (topicProbs, wordInd) => (wordMap(wordInd), topicProbs) }).toMap
  }

  def formatSparkLDADocTopicOutput(docTopDist: RDD[(Long, Vector)], docDict: Map[Int, String]):
  RDD[(String, Array[Double])] = {
    docTopDist.map({ case (docID, topicVal) => (docDict(docID.toInt), topicVal.toArray) })
  }

}