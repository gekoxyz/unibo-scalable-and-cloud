package it.matteogaliazzo.spark 

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.math.BigDecimal

case class Position(longitude: Double, latitude: Double) extends Ordered[Position] {
  // We need to implement sorting to ensure Pair(A,B) is the same as Pair(B,A)
  def compare(that: Position): Int = {
    val latComp = this.latitude.compare(that.latitude)
    if (latComp != 0) latComp else this.longitude.compare(that.longitude)
  }
  override def toString: String = s"($latitude, $longitude)"
}
//case class Position(longitude: String, latitude: String)
case class EventKey(position: Position, date: String)

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("EarthQuake-CoOccurrence")
      .master("local[*]").getOrCreate()

    val sc = spark.sparkContext
    // Reduce console clutter
    sc.setLogLevel("WARN")
    // For syntactic sugar and automatic conversions
    import spark.implicits._

    val df = spark.read.option("header", "true").csv("Datasets/dataset-earthquakes-trimmed.csv")
    val rdd = df.as[(String, String, String)].rdd // Convert DataFrame to RDD

    // create RDD with UNIQUE events (unique is granted by distinct)
    val uniqueEventsRdd = rdd.map { row =>
      val lat = row._1.toDouble
      val lon = row._2.toDouble
      val rawDate = row._3

      val newLat = BigDecimal(lat).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
      val newLon = BigDecimal(lon).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble

      val newDate = rawDate.trim.split(" ")(0)

      EventKey(Position(newLon, newLat), newDate)
    }.distinct()

    // Group by Date to find daily clusters
    val groupedByDate = uniqueEventsRdd
      .map(e => (e.date, e.position))
      .groupByKey()

    // Generate Pairs (The "Edge" creation)
    // We map Date -> [PosA, PosB, PosC] into:
    // ((PosA, PosB), Date), ((PosA, PosC), Date), ((PosB, PosC), Date)
    val pairDateRdd: RDD[((Position, Position), String)] = groupedByDate.flatMap { case (date, positions) =>
      val posList = positions.toList.sorted // Sorting is crucial for canonical pairs

      // Generate all unique combinations of size 2
      posList.combinations(2).map {
        case List(p1, p2) => ((p1, p2), date)
      }
    }

    // Map the Iterable to its size (the frequency count)
    val pairCounts = pairDateRdd
      .groupByKey()
      .mapValues(dates => dates.size) // This converts Seq("date1", "date2") into 2

    // Sort by frequency descending and print
    val topPairs = pairCounts.sortBy(_._2, ascending = false)

    println("--- Top 10 Co-Occurring Pairs ---")
    topPairs.take(10).foreach { case (pair, count) =>
      println(s"Pair: $pair | Frequency: $count")
    }

//    println("--- RDD grouped by date ---")
//    groupedByDate.take(100).foreach { case (date, positions) =>
//      println(s"Date: $date | Positions: ${positions.mkString(", ")}")
//    }
//    println("---------------------------")


    //    scala.io.StdIn.readLine()
    spark.stop()
  }
}