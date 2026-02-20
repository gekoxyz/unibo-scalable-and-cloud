package it.matteogaliazzo.spark 

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.math.BigDecimal

case class Position(longitude: Int, latitude: Int) extends Ordered[Position] {
  def compare(that: Position): Int = {
    val latComp = this.latitude.compare(that.latitude)
    if (latComp != 0) latComp else this.longitude.compare(that.longitude)
  }
  override def toString: String = s"(${latitude/10.0}, ${longitude/10.0})"
}

case class EventKey(position: Position, date: Int)

object Main {
  def main(args: Array[String]): Unit = {

    val inputPath = if (args.length > 0) args(0) else "Datasets/dataset-earthquakes-trimmed.csv"
    val outputPath = if (args.length > 1) args(1) else "output_results"
    val numPartitions = if (args.length > 2) args(2).toInt else 8

    val spark = SparkSession.builder().appName("EarthQuake-CoOccurrence").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val df = spark.read.option("header", "true").csv(inputPath).repartition(numPartitions)

    val rawRdd = df.rdd.map { row =>
      val latRaw = row.getString(0).toDouble
      val lonRaw = row.getString(1).toDouble
      val dateStr = row.getString(2) // "2026-02-17 18:19:00"

      val latInt = math.round(latRaw * 10).toInt
      val lonInt = math.round(lonRaw * 10).toInt

      // "2026-02-17..." -> indices 0-4, 5-7, 8-10
      val year = dateStr.substring(0, 4).toInt
      val month = dateStr.substring(5, 7).toInt
      val day = dateStr.substring(8, 10).toInt
      val dateInt = (year * 10000) + (month * 100) + day

      (dateInt, Position(lonInt, latInt))
    }.distinct(numPartitions)

    val groupedByDate = rawRdd
      .aggregateByKey(Set.empty[Position])(
        (set, pos) => set + pos, // local combine on each partition
        (set1, set2) => set1 ++ set2 // merge across partitions
      ).filter(_._2.size >= 2).mapValues(_.toList.sorted).cache()

    val pairCounts = groupedByDate.flatMap { case (_, positions) =>
      positions.combinations(2).map { pair =>
        ((pair(0), pair(1)), 1)
      }
    }.reduceByKey(_ + _, numPartitions)

    val (bestPair, maxCount) = pairCounts.reduce { (a, b) =>
      if (a._2 > b._2) a else b
    }

    val (p1, p2) = bestPair
    val winningDates = groupedByDate.filter { case (_, positions) =>
        positions.contains(p1) && positions.contains(p2)
      }.map { case (date, _) => date }
      .collect()
      .sorted

    val resultLines = Seq(s"Top Pair: ($p1, $p2)", s"Frequency: $maxCount") ++
      winningDates.map { d =>
        val s = d.toString
        s"${s.substring(0,4)}-${s.substring(4,6)}-${s.substring(6,8)}"
      }

    sc.parallelize(resultLines, 1).saveAsTextFile(outputPath)

    // To keep localhost:4040 alive to check the cluster job before doing spark.stop
    // scala.io.StdIn.readLine()
    spark.stop()
  }
}