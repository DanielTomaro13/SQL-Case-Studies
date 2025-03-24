Task 2 
Find the most frequently occurring word for each document and then
output the following information: docId, word, count. Sort in decreasing order
according to count.

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

// Define case classes for input data
case class Docword(docId: Int, vocabId: Int, count: Int)
case class VocabWord(vocabId: Int, word: String)

object Main {
  def solution(spark: SparkSession) {
    import spark.implicits._

    val docwords = spark.read.
      schema(Encoders.product[Docword].schema).
      option("delimiter", " ").
      csv("/docword.txt").
      as[Docword]

    val vocab = spark.read.
      schema(Encoders.product[VocabWord].schema).
      option("delimiter", " ").
      csv("/vocab.txt").
      as[VocabWord]

    // TODO: *** Put your solution here ***
    val windowSpec = Window.partitionBy("docId").orderBy(desc("count"))

    val result = docwords
      .join(vocab, "vocabId")
      .withColumn("rank", dense_rank().over(windowSpec))
      .filter($"rank" === 1)
      .select($"docId", $"word", $"count")
      .orderBy(desc("count"))

    // Write to CSV
    result.write
      .option("header", "false")
      .csv("Task_3b-out")
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task3b")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    // Run solution code
    solution(spark)
    // Stop Spark
    spark.stop()
  }
}

WITH ranked_words AS (
  SELECT
    d.docId,
    v.word,
    d.count,
    DENSE_RANK() OVER (PARTITION BY d.docId ORDER BY d.count DESC) AS rnk
  FROM
    docword d
  JOIN
    vocab v ON d.vocabId = v.vocabId
)
SELECT
  docId,
  word,
  count
FROM
  ranked_words
WHERE
  rnk = 1
ORDER BY
  count DESC
