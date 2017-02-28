package com.challenges.elastictry

import java.sql.Timestamp
import java.util.Calendar

import com.thesamet.spatial.KDTree
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

import scala.collection.{Map, mutable}
import scala.math.{pow, sqrt}
import scala.util.Try

/**
  * MEET Criterion for two UIDs:
  * If two uids are:
  *     1. on the same floor,
  *     2. within 5 minutes of each other, 
  *     3. within 3 metres of each other,
  * then a meeting is said to have occurred between them.
  *
  * Usage: "/usr/local/Cellar/apache-spark/2.1.0/bin/spark-submit
  * --class "com.challenges.elastictry.MeetingDecisionEngine"
  * --master local[4]
  * target/scala-2.11/meetdecisionengine-v1.jar <uid1> <uid2>"
  */

object MeetingDecisionEngine {

  private val THRESHOLD_MEET_TIME_MINS: Int = 5
  private val THRESHOLD_MEET_DIST_METRES: Int = 3

  def GetInputDataset(spark: SparkSession, inFile: String, inSchema: StructType) : Dataset[EventsRaw] = {
    import spark.implicits._

    spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .schema(inSchema)
      .load(inFile)
      .as[EventsRaw]
  }

  def EuclideanDist(x1:Double, y1:Double, x2:Double, y2:Double) : Double = {
    sqrt( pow((x1 - x2),2) + pow((y1-y2),2) )
  }


  def GetPerUserDataset(spark: SparkSession, inDs: Dataset[EventsRaw])
  : (Dataset[PerUserMeetingLocation], Map[Int, Long], Map[(Int, Int), Int]) = {
    val tmpMinTs = new DateTime(inDs.agg(min(col("ts"))).take(1)(0).getTimestamp(0).getTime)
    val minTs = new DateTime(tmpMinTs.getYear, tmpMinTs.getMonthOfYear, tmpMinTs.getDayOfMonth,
      tmpMinTs.getHourOfDay, tmpMinTs.getMinuteOfHour)

    val tmpMaxTs = new DateTime(inDs.agg(max(col("ts"))).take(1)(0).getTimestamp(0).getTime)
    val maxTs = new DateTime(tmpMaxTs.getYear, tmpMaxTs.getMonthOfYear, tmpMaxTs.getDayOfMonth,
      tmpMaxTs.getHourOfDay, tmpMaxTs.getMinuteOfHour)

    val minFlr = inDs.agg(min(col("flr"))).take(1)(0).getInt(0)
    val maxFlr = inDs.agg(max(col("flr"))).take(1)(0).getInt(0)
    val flrs = (minFlr to maxFlr).toSeq

    // Create map of (index -> meeting start time)
    val mapIdxToMeetStart: Map[Int, Long] = (minTs.getMillis to maxTs.getMillis by THRESHOLD_MEET_TIME_MINS*60*1000)
      .zipWithIndex.toMap.map(_.swap)

    val mapFlrMeetIdxToIdx: Map[(Int, Int), Int] = flrs.map(f => {
      val sortedMeetIdxs = mapIdxToMeetStart.keysIterator.toArray.sortWith(_ < _)
      (sortedMeetIdxs(0) to sortedMeetIdxs(sortedMeetIdxs.length-1)).map(m => (f,m))
    } ).flatten.zipWithIndex.toMap

    def ConvertMillisToMeetIdx(inMillis: Long): Int = ((inMillis - minTs.getMillis)/(THRESHOLD_MEET_TIME_MINS*60*1000L)).toInt

    import spark.implicits._
    (spark.createDataset(
      inDs.map({ case obj => (obj.uid, obj.x, obj.y, obj.flr, {
        val tsMillis: Long = {
          val cal = Calendar.getInstance()
          cal.setTime(obj.ts)
          cal.getTimeInMillis
        }
        val dt = new DateTime(tsMillis)
        val preTime = dt.minusMinutes(THRESHOLD_MEET_TIME_MINS).getMillis
        val postTime = dt.plusMinutes(THRESHOLD_MEET_TIME_MINS).getMillis

        val setMeetIdxs: mutable.Set[Int] = mutable.Set[Int](ConvertMillisToMeetIdx(tsMillis))

        if (preTime >= minTs.getMillis ) setMeetIdxs ++= Set(ConvertMillisToMeetIdx(preTime))
        if (postTime <= maxTs.getMillis) setMeetIdxs ++= Set(ConvertMillisToMeetIdx(postTime))
        setMeetIdxs.toSeq
      })
      })
        .map({ case (uid, x, y, flr, meetIdxs) =>
          ( uid, meetIdxs.map({ case idx => mapFlrMeetIdxToIdx((flr, idx)) }).map(idx => (idx, (x,y))).toMap )
        })
        .rdd
        .groupBy(_._1)
        .map({ case obj => PerUserMeetingLocation(
          obj._1,
          obj._2.map(_._2).flatten.groupBy(_._1).map({ case obj1 => (obj1._1 -> obj1._2.map(_._2).toSet.toSeq) })
        ) })
    ).as[PerUserMeetingLocation]
      , mapIdxToMeetStart
      , mapFlrMeetIdxToIdx
      )
  }

  def GetUsersJoinedLocations(spark: SparkSession, inDs: Dataset[PerUserMeetingLocation])
  : Dataset[JoinedUsersLocations] = {
    val arrUids: Array[String] = inDs.select(col("uid")).distinct().take(2).map(r => r.getString(0))

    val mapUidToDatasetLocns: Map[String, Dataset[UserFlrMeetIdxLocnsExploded]] = arrUids.map({case inUid => (inUid -> {
      import spark.implicits._
      inDs.where(col("uid")===inUid)
        .map({ case obj => ({
          obj.mapFlrMeetIdxToLocns.keys.toArray
            .map({ case k =>
              (obj.uid, k, obj.mapFlrMeetIdxToLocns(k))
            })
        })
        })
        .withColumn("exploded_col", explode(col("value")))
        .drop("value")
        .rdd.map(r => {
        val rowObj = r.getStruct(0)
        val locns: Seq[(Double, Double)] = rowObj.getSeq[Row](2).map({ case r => (r.getDouble(0), r.getDouble(1)) })
        UserFlrMeetIdxLocnsExploded(rowObj.getString(0)
          , rowObj.getInt(1)
          , locns
        )
      })
        .toDF("uid", "flrMeetIdx", "locns")
        .as[UserFlrMeetIdxLocnsExploded]
    })
    }).toMap

    import spark.implicits._
    mapUidToDatasetLocns(arrUids(0))
      .joinWith(mapUidToDatasetLocns(arrUids(1)),
        mapUidToDatasetLocns(arrUids(0)).col("flrMeetIdx")===mapUidToDatasetLocns(arrUids(1)).col("flrMeetIdx"))
      .map(r => JoinedUsersLocations(r._1.flrMeetIdx, r._1.locns, r._2.locns))
      .toDF("flrMeetIdx", "user1Locations", "user2Locations")
      .as[JoinedUsersLocations]
  }

  def IsMeetingPossible(spark: SparkSession, inDs: Dataset[JoinedUsersLocations]): (Boolean, Option[Int]) = {
    val arrLocns = inDs.rdd.collect()

    arrLocns.map(obj => {
      if (obj.user1Locations.length > obj.user2Locations.length) {
        // insert locns1 into KD-tree
        val tree = KDTree.fromSeq(obj.user1Locations)
        obj.user2Locations.map({case p => {
          val nn = tree.findNearest(p, 1)
          val dist = EuclideanDist(p._1, p._2, nn.apply(0)._1, nn.apply(0)._2)
          if (dist > 0 && dist <= THRESHOLD_MEET_DIST_METRES) return (true, Some(obj.flrMeetIdx))
        } })
      } else {
        // put locns2 into KD-tree
        val tree = KDTree.fromSeq(obj.user2Locations)
        obj.user1Locations.map({case p => {
          val nn = tree.findNearest(p, 1)
          val dist = EuclideanDist(p._1, p._2, nn.apply(0)._1, nn.apply(0)._2)
          if (dist > 0 && dist <= THRESHOLD_MEET_DIST_METRES) return (true, Some(obj.flrMeetIdx))
        } })
      }
    })

    (false, None)
  }

  def main(args: Array[String]): Unit = {
    val spark : SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Meeting-DecisionEngine")
      .getOrCreate()

    val uid1 = if (Try(args(0).trim.hasDefiniteSize).isSuccess) args(0).trim else throw new Exception("ERROR: Please enter valid UID!")
    val uid2 = if (Try(args(1).trim.hasDefiniteSize).isSuccess) args(1).trim else throw new Exception("ERROR: Please enter valid UID!")

    if (uid1 == uid2) throw new Exception("ERROR: Please select two unique UIDs")

    val inFile : String = "data/reduced.csv"

    val inSchema = new StructType()
      .add("ts", TimestampType)
      .add("x", DoubleType)
      .add("y", DoubleType)
      .add("flr", IntegerType)
      .add("uid", StringType)

    val dsIn = GetInputDataset(spark, inFile, inSchema)
    import spark.implicits._
    val arrUids: Array[String] = dsIn.select(col("uid")).distinct().map(r => r.getString(0)).collect()

    if (arrUids.contains(uid1) && arrUids.contains(uid2)) {
      val (dsUserMeetingLocns, mapIdxToMeetStart, mapFlrMeetIdxToIdx) = GetPerUserDataset(spark,
        dsIn.where(col("uid")===uid1 || col("uid")===uid2))

      val dsJoinedUsersLocns = GetUsersJoinedLocations(spark, dsUserMeetingLocns)

      val (meetFlag, flrMeetIdx) = IsMeetingPossible(spark, dsJoinedUsersLocns)

      if (meetFlag) {
        val (flr, meetIdx) = mapFlrMeetIdxToIdx.map(_.swap).apply(flrMeetIdx.get)
        val meetTime = new DateTime(mapIdxToMeetStart(meetIdx))
        //TODO: JSON output append of results of runs
        println(s"+++++++++++ RESULT: Users could've met on floor ${flr} at around ${meetTime} +++++++++++")
      } else println(s"+++++++++++ RESULT: Users couldn't have met! +++++++++++")
    } else throw new Exception("ERROR: Please enter valid UID(s)!")

    spark.stop()
  }

  case class EventsRaw(
    ts: Timestamp,
    x: Double,
    y: Double,
    flr: Int,
    uid: String
  )

  case class PerUserMeetingLocation(
    uid: String,
    mapFlrMeetIdxToLocns: Map[ Int, Seq[ (Double, Double) ] ]
  )

  case class UserFlrMeetIdxLocnsExploded(
    uid: String,
    flrMeetIdx: Int,
    locns: Seq[(Double, Double)]
  )

  case class JoinedUsersLocations(
    flrMeetIdx: Int,
    user1Locations: Seq[(Double, Double)],
    user2Locations: Seq[(Double, Double)]
  )

}