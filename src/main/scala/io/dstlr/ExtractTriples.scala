package io.dstlr

import java.net.URL
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import edu.stanford.nlp.ie.util.RelationTriple
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreAnnotations.DocDateAnnotation
import edu.stanford.nlp.pipeline.{CoreDocument, CoreEntityMention, CoreSentence, StanfordCoreNLP}
import edu.stanford.nlp.simple.Document
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, Map => MMap}
import scala.xml.XML

/**
  * Extract raw triples from documents on Solr using CoreNLP.
  */
object ExtractTriples {

  // Used for the full NER, KBP, and entity linking
  @transient lazy val nlp = new StanfordCoreNLP(new Properties() {
    {
      setProperty("annotators", "tokenize,ssplit,pos,lemma,parse,ner,coref,kbp,entitylink")
      setProperty("coref.algorithm", "statistical")
      setProperty("threads", "8")
      setProperty("parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz")
    }
  })

  val printFormat = new SimpleDateFormat("yyyy-MM-dd")

  def main(args: Array[String]): Unit = {

    // Setup config
    val conf = new Conf(args)
    println(conf.summary)

    // Build the SparkSession
    val spark = SparkSession
      .builder()
      .appName("dstlr - ExtractTriples")
      .getOrCreate()

    // Import implicit functions from SparkSession
    import spark.implicits._

    // Accumulators for keeping track of # docs, tokens, and triples
    val doc_acc = spark.sparkContext.longAccumulator("docs")
    val token_acc = spark.sparkContext.longAccumulator("tokens")
    val triple_acc = spark.sparkContext.longAccumulator("triples")

    // Delete old output directory
    FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new Path(conf.output()), true)

    // Start time
    val start = System.currentTimeMillis()

    val ds = if (conf.solr()) {
      solr(spark, conf)
    } else {
      text(spark, conf)
    }

    val result = ds
      .repartition(conf.partitions())
      .filter(doc => doc.getAs[String]("id") != null && doc.getAs[String]("id").nonEmpty)
      .filter(doc => doc.getAs[String]("contents") != null && doc.getAs[String]("contents").nonEmpty)
      .filter(doc => new Document(doc.getAs[String]("contents")).sentences().forall(_.tokens().size() <= conf.sentLengthThreshold()))
      .mapPartitions(part => {

        // The extracted triples
        val triples = new ListBuffer[TrainingData]()

        val mentions = MMap[String, CoreEntityMention]()

        val mapped = part.map(row => {

          println(s"${System.currentTimeMillis()} - Processing ${row.getAs[String]("id")} on ${Thread.currentThread().getName()}")

          // The extracted triples
          triples.clear()

          // Mentions
          mentions.clear()

          // Increment # of docs
          doc_acc.add(1)

          try {

            // Create and annotate the CoreNLP Document
            val doc = new CoreDocument(row.getAs[String]("contents"))

            try {
              if (row.getAs[Long]("published_date") != null && row.getAs[Long]("published_date") >= 0) {
                doc.annotation().set(classOf[DocDateAnnotation], printFormat.format(new Date(row.getAs[Long]("published_date"))))
              }
            } catch {
              case e: Exception => println(s"No error parsing published_date from ${row.getAs("id")}")
            }

            nlp.annotate(doc)

            // Increment # tokens
            token_acc.add(doc.tokens().size())

            // For each sentence...
            doc.sentences().foreach(sentence => {

              sentence.entityMentions().foreach(mention => {
                mentions.put(toLemmaString(mention), mention)
              })

              // Extract the relations between entities.
              sentence.relations().filter(relation => relation.relationGloss() == "per:date_of_death").foreach(relation => {
                if (mentions.contains(relation.subjectLemmaGloss()) && mentions.contains(relation.objectLemmaGloss())) {
                  triples.append(buildRelation(row.getAs[String]("id"), sentence, mentions, relation))
                }
              })
            })

          } catch {
            case e: Exception => println(s"Exception when processing ${row.getAs[String]("id")} - ${e}")
          }

          // Increment # triples
          triple_acc.add(triples.size())

          triples.toList

        })

        // Log timing info
        println(nlp.timingInformation())

        mapped

      })
      .flatMap(x => x)

    // Write to parquet file
    result.write.parquet(conf.output())

    val duration = System.currentTimeMillis() - start
    println(s"Took ${duration}ms @ ${doc_acc.value / (duration / 1000)} doc/s, ${token_acc.value / (duration / 1000)} token/s, and ${triple_acc.value / (duration / 1000)} triple/sec")

    spark.stop()

  }

  def text(spark: SparkSession, conf: Conf): Dataset[Row] = {

    import spark.implicits._

    // Test data
    spark.sparkContext.parallelize(Seq("Steven Hawking died on March 14, 2018. He died on 14 March 2018.", "Joseph S. Handler died on April 14, 2019."))
      .zipWithIndex()
      .map(_.swap)
      .toDF("id", "contents")

  }

  def solr(spark: SparkSession, conf: Conf): Dataset[Row] = {

    import spark.implicits._

    val options = Map(
      "collection" -> conf.solrIndex(),
      "query" -> conf.query(),
      "rows" -> conf.rows(),
      "zkhost" -> conf.solrUri()
    )

    // Create a DataFrame with the query results
    spark.read.format("solr")
      .options(options)
      .load()

  }

  def toLemmaString(mention: CoreEntityMention): String = {
    mention.tokens()
      .filter(x => !x.tag.matches("[.?,:;'\"!]"))
      .map(token => if (token.lemma() == null) token.word() else token.lemma())
      .mkString(" ")
  }

  def buildMention(doc: String, uuid: String, mention: CoreEntityMention): TripleRow = {

    val meta = MMap(
      "class" -> mention.entityType(),
      "span" -> mention.text(),
      "begin" -> mention.charOffsets().first.toString,
      "end" -> mention.charOffsets().second.toString
    )

    val entityType = mention.entityType()

    // If the entity is annotated by SUTIME, save the normalized time.
    if (entityType == "DATE" || entityType == "DURATION" || entityType == "TIME" || entityType == "SET") {
      meta("normalized") = mention.coreMap().get(classOf[CoreAnnotations.NormalizedNamedEntityTagAnnotation])
    }

    new TripleRow(doc, "Document", doc, "MENTIONS", "Mention", uuid, meta.toMap)
  }

  def buildLinksTo(doc: String, mention: String, uri: String): TripleRow = {
    new TripleRow(doc, "Mention", mention, "LINKS_TO", "Entity", uri, null)
  }

  def getUri(text: String): String = {
    val data = XML.load(new URL(s"http://192.168.1.110:1111/api/search/KeywordSearch?MaxHits=1&QueryString=${text}"))
    val uri = (data \ "Result" \ "URI").text
    uri.substring(uri.lastIndexOf("/") + 1, uri.length)
  }

  def buildRelation(doc: String, sentence: CoreSentence, mentions: MMap[String, CoreEntityMention], triple: RelationTriple): TrainingData = {

    val subUri = getUri(mentions(triple.subjectLemmaGloss()).text())
    val objUri = getUri(mentions(triple.objectLemmaGloss()).text())

    val sub = new EntityData(
      triple.subjectTokenSpan().first(),
      triple.subjectTokenSpan().second(),
      triple.subjectGloss(),
      mentions(triple.subjectLemmaGloss()).entityType(),
      subUri,
      null
    )

    val entityType = mentions(triple.objectLemmaGloss()).entityType()
    var normalized: String = null

    // If the entity is annotated by SUTIME, save the normalized time.
    if (entityType == "DATE" || entityType == "DURATION" || entityType == "TIME" || entityType == "SET") {
      normalized = mentions(triple.objectLemmaGloss()).coreMap().get(classOf[CoreAnnotations.NormalizedNamedEntityTagAnnotation])
    }

    val obj = new EntityData(
      triple.objectTokenSpan().first(),
      triple.objectTokenSpan().second(),
      triple.objectGloss(),
      mentions(triple.objectLemmaGloss()).entityType(),
      objUri,
      normalized
    )

    new TrainingData(triple.relationGloss(), triple.confidence, false, sub, obj, sentence.tokens().map(_.originalText()).toList)

  }
}