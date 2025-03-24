Task 1
Find the hash tag name that has increased the number of
tweets the most from among any two consecutive months of any hash tag name.
Consecutive month means for example, 200801 to 200802, or 200902 to 200903, etc.
Report the hash tag name, the 1st month count, and the 2nd month count using println.

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

object Main {
  def solution(sc: SparkContext) {
    // Load each line of the input data
    val twitterLines = sc.textFile("/twitter.tsv")
    // Split each line of the input data into an array of strings
    val twitterdata = twitterLines.map(_.split("\t"))
    
    // TODO: *** Put your solution here ***
    val result = twitterdata
      .map(row => (row(3), (row(1), row(2).toInt))) // (hashtag, (month, count))
      .groupByKey()
      .flatMap { case (hashtag, counts) =>
        val sortedCounts = counts.toList.sortBy(_._1) // Sort by month
        sortedCounts.zip(sortedCounts.tail).flatMap { case ((month1, count1), (month2, count2)) =>
          if (isConsecutive(month1, month2) && count2 > count1) {
            Some((hashtag, month1, month2, count1, count2, count2 - count1))
          } else {
            None
          }
        }
      }
      .reduce((a, b) => if (a._6 > b._6) a else b) // Find max increase
    
    println(s"Hash tag name: ${result._1}")
    println(s"count of month ${result._2}: ${result._4}")
    println(s"count of month ${result._3}: ${result._5}")
  }
  
  // Helper function to check if two months are consecutive
  def isConsecutive(month1: String, month2: String): Boolean = {
    val (year1, m1) = (month1.take(4).toInt, month1.drop(4).toInt)
    val (year2, m2) = (month2.take(4).toInt, month2.drop(4).toInt)
    
    (year1 == year2 && m2 - m1 == 1) || (year2 - year1 == 1 && m1 == 12 && m2 == 1)
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("TaskBonus1")
      .master("local[4]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.default.parallelism", 4)
      .getOrCreate()
    // Run solution code
    solution(spark.sparkContext)
    // Stop Spark
    spark.stop()
  }
}


WITH ordered_counts AS (
  SELECT
    hashtag_name,
    month,
    count,
    LEAD(month) OVER (PARTITION BY hashtag_name ORDER BY month) AS next_month,
    LEAD(count) OVER (PARTITION BY hashtag_name ORDER BY month) AS next_count
  FROM twitter
),

consecutive_diffs AS (
  SELECT
    hashtag_name,
    month AS month1,
    next_month AS month2,
    count AS count1,
    next_count AS count2,
    (next_count - count) AS diff
  FROM ordered_counts
  WHERE
    is_consecutive(month, next_month)
    AND next_count > count
),

max_increase AS (
  SELECT *
  FROM consecutive_diffs
  ORDER BY diff DESC
  LIMIT 1
)

SELECT
  hashtag_name,
  month1,
  count1,
  month2,
  count2
FROM max_increase;
