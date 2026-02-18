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

case class EventKey(position: Position, date: Int)

object Main {
  def main(args: Array[String]): Unit = {

    val inputPath = if (args.length > 0) args(0) else "Datasets/dataset-earthquakes-trimmed.csv"
    val outputPath = if (args.length > 1) args(1) else "output_results"
    val numPartitions = if (args.length > 2) args(2).toInt else 8

    val spark = SparkSession.builder().appName("EarthQuake-CoOccurrence").getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    val df = spark.read.option("header", "true").csv(inputPath)

    // create RDD with UNIQUE events (unique is granted by distinct)
    // We repartition immediately after loading to spread the load
    val uniqueEventsRdd = df.rdd.repartition(numPartitions).map { row =>
      val latRaw = row.getString(0).toDouble
      val lonRaw = row.getString(1).toDouble
      val dateRaw = row.getString(2) // "2026-02-17 18:19:00"

      // Optimization: round and store as Int, and convert Date String to Int
      val latInt = math.round(latRaw * 10).toInt
      val lonInt = math.round(lonRaw * 10).toInt
      val dateInt = dateRaw.trim.split(" ")(0).replace("-", "").toInt

      EventKey(Position(lonInt, latInt), dateInt)
    }.distinct()

    // 3. Group by Date & Cache
    // This puts the lists [PosA, PosB, ...] in memory.
    val groupedByDate = uniqueEventsRdd
      .map(e => (e.date, e.position))
      .groupByKey()
      .cache()

    // 4. Pass 1: Count Co-Occurrences (The Heavy Lifting)
    // We generate combinations ONLY for counting.
    // reduceByKey is where the real "heavy lifting" happens.
    // Passing numPartitions here controls how many "tasks" run during the count.
    val pairCounts = groupedByDate.flatMap { case (_, positions) =>
      val posList = positions.toList.sorted
      posList.combinations(2).map { pair =>
        ((pair(0), pair(1)), 1)
      }
    }.reduceByKey(_ + _, numPartitions) // Efficient Combiner

    if (pairCounts.isEmpty()) {
      println("No co-occurrences found.")
      spark.stop()
      return
    }

    // 5. Find the Winner
    val (bestPair, maxCount) = pairCounts.reduce { (a, b) =>
      if (a._2 > b._2) a else b
    }

    // 6. Pass 2: Retrieve Dates (OPTIMIZED for CPU)
    // We do NOT regenerate combinations. We scan the cached lists.
    val (p1, p2) = bestPair

    val winningDates = groupedByDate.filter { case (_, positions) =>
        val posList = positions.toList
        // Check existence (O(N)) instead of generating combinations (O(N^2))
        posList.contains(p1) && posList.contains(p2)
      }.map { case (date, _) => date }
      .collect()
      .sorted

    // 7. Format Output String
    val resultBuilder = new StringBuilder
    resultBuilder.append(s"Top Pair: $bestPair\n")
    resultBuilder.append(s"Frequency: $maxCount\n")
    resultBuilder.append("Dates:\n")

    winningDates.foreach { d =>
      val s = d.toString
      // Format 20240312 -> 2024-03-12
      resultBuilder.append(s"${s.substring(0,4)}-${s.substring(4,6)}-${s.substring(6,8)}\n")
    }

    val finalOutput = resultBuilder.toString()

    // 8. Save to File (Cloud Storage)
    // This is the cleanest way to get your result from DataProc.
    // It writes a single file containing your formatted string.
    sc.parallelize(Seq(finalOutput), 1).saveAsTextFile(outputPath)

    // To keep localhost:4040 alive to check the cluster job before doing spark.stop
    // scala.io.StdIn.readLine()
    spark.stop()
  }
}