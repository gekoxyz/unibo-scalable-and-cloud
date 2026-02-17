package it.matteogaliazzo.spark 

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.math.BigDecimal

case class Position(longitude: Int, latitude: Int) extends Ordered[Position] {
  // We need to implement sorting to ensure Pair(A,B) is the same as Pair(B,A)
  def compare(that: Position): Int = {
    val latComp = this.latitude.compare(that.latitude)
    if (latComp != 0) latComp else this.longitude.compare(that.longitude)
  }
  override def toString: String = s"(${latitude/10.0}, ${longitude/10.0})"
}
//case class Position(longitude: String, latitude: String)
case class EventKey(position: Position, date: Int)

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("EarthQuake-CoOccurrence")
      .master("local[*]").getOrCreate()

    val sc = spark.sparkContext
    // Reduce console clutter
    sc.setLogLevel("WARN")
    // For syntactic sugar and automatic conversions
    import spark.implicits._

//    val df = spark.read.option("header", "true").csv("Datasets/test.csv")
    val df = spark.read.option("header", "true").csv("Datasets/dataset-earthquakes-trimmed.csv")
    val rdd = df.as[(String, String, String)].rdd // Convert DataFrame to RDD

    // create RDD with UNIQUE events (unique is granted by distinct)
    val uniqueEventsRdd = rdd.map { row =>
      val latRaw = row._1.toDouble
      val lonRaw = row._2.toDouble
      val dateRaw = row._3 // "2026-02-17 18:19:00"

      // Optimization: Multiply by 10, round, and store as Int
      // Example: 37.521 -> 375.21 -> 375
      val latInt = math.round(latRaw * 10).toInt
      val lonInt = math.round(lonRaw * 10).toInt

      val dateInt = dateRaw.trim.split(" ")(0).replace("-", "").toInt

      EventKey(Position(lonInt, latInt), dateInt)
    }.distinct()

    // Group by Date to find daily cluster pairs
    // We cache this because we need it for the second pass
    val groupedByDate = uniqueEventsRdd
      .map(e => (e.date, e.position))
      .groupByKey()
      .cache() // TODO: check if this is the same as what we have seen during the course with persist

    // Generate Pairs (The "Edge" creation)
    // We map Date -> [PosA, PosB, PosC] into:
    // ((PosA, PosB), Date), ((PosA, PosC), Date), ((PosB, PosC), Date)
    val pairDateRdd = groupedByDate.flatMap { case (date, positions) =>
      val posList = positions.toList.sorted
      posList.combinations(2).map {
        case List(p1, p2) => ((p1, p2), date)
      }
    }

    // Map to count 1, then ReduceByKey to sum. This minimizes shuffle.
    val pairCounts = pairDateRdd
      .map{case (pair, _) => (pair, 1)}
      .reduceByKey(_ + _)

    val (bestPair, maxCount) = pairCounts.reduce { (a, b) =>
      if (a._2 > b._2) a else b
    }

    // Retrieve Dates for the Winner
    // We go back to the pairDateRdd (or re-derive) and filter ONLY for the winner.
    // This avoids carrying date lists for the millions of losing pairs.
    val bestPairDates = pairDateRdd
      .filter { case (pair, _) => pair == bestPair }
      .map { case (_, date) => date }
      .collect()
      .sorted // REQUIREMENT: Sort dates ascending

    println(s"Top Pair: $bestPair")
    println(s"Frequency: $maxCount")
    println("Dates:")
    bestPairDates.foreach(println)

    // To keep localhost:4040 alive to check the cluster job before doing spark.stop
    scala.io.StdIn.readLine()
    spark.stop()
  }
}