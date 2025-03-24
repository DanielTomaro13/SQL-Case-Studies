Task 1
Calculate the total count of each word across all documents. List the
words in ascending alphabetical order. Write the results to “Task_3a-out” in CSV
format.

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

    val result = docwords
      .join(vocab, "vocabId")
      .groupBy("word")
      .agg(sum("count").as("total_count"))
      .orderBy("word")

    // Write to CSV
    result
      .select($"word", $"total_count")
      .write
      .option("header", "false")
      .csv("Task_3a-out")
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task3a")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    // Run solution code
    solution(spark)
    // Stop Spark
    spark.stop()
  }
}

SELECT
  v.word,
  SUM(d.count) AS total_count
FROM
  docword d
JOIN
  vocab v ON d.vocabId = v.vocabId
GROUP BY
  v.word
ORDER BY
  v.word ASC
