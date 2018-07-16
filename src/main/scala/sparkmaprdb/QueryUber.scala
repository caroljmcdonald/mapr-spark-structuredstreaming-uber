package sparkmaprdb

import org.apache.spark._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import com.mapr.db._
import com.mapr.db.spark._
import com.mapr.db.spark.impl._
import com.mapr.db.spark.sql._
import org.apache.log4j.{ Level, Logger }

object QueryUber {

  case class UberwId(_id: String, dt: java.sql.Timestamp,
    lat: Double, lon: Double, base: String, cid: Integer,
    clat: Double, clon: Double) extends Serializable

  val schema = StructType(Array(
    StructField("_id", StringType, true),
    StructField("dt", TimestampType, true),
    StructField("lat", DoubleType, true),
    StructField("lon", DoubleType, true),
    StructField("base", StringType, true),
    StructField("cid", IntegerType, true),
    StructField("clat", DoubleType, true),
    StructField("clon", DoubleType, true)
  ))

  def main(args: Array[String]) {

    var tableName: String = "/apps/ubertable"
    if (args.length == 1) {
      tableName = args(0)
    } else {
      System.out.println("Using hard coded parameters unless you specify the tablename ")
    }
    val spark: SparkSession = SparkSession.builder().appName("uber").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("OFF")
    Logger.getLogger("org").setLevel(Level.OFF)

    import spark.implicits._
    // load payment dataset from MapR-DB 



      val df: Dataset[UberwId] = spark
        .loadFromMapRDB[UberwId](tableName, schema)
        .as[UberwId]
      df.createOrReplaceTempView("uber")

      println("Count of rows" + df.count)

      println("Show the first 20 rows")
      df.show

      println("Display datetime and cluster counts for Uber trips")
      spark.sql("select cid, dt, count(cid) as count from uber group by dt, cid order by dt, cid limit 100 ").show

      println("Which hours have the highest pickups for cluster id 0 ?")
      df.filter($"_id" <= "1")
        .select(hour($"dt").alias("hour"), $"cid")
        .groupBy("hour", "cid").agg(count("cid")
          .alias("count")).orderBy(desc("count")).show

      println("Which cluster locations have the highest number of pickups?")
      df.groupBy("cid").count().orderBy(desc("count")).show

      println("Which hours and cluster locations have the highest number of pickups?")
      df.select(hour($"dt").alias("hour"), $"cid")
        .groupBy("hour", "cid").agg(count("cid")
          .alias("count")).orderBy(desc("count")).show

      println("Which day, and hour have the highest number of pickups?")
      df.select(dayofmonth($"dt").alias("day"), hour($"dt").alias("hour"), $"cid").groupBy("day", "hour", "cid").agg(count("cid").alias("count")).orderBy(desc("count")).show
      /*
      println("Which hours have the highest number of pickups?")
      spark.sql("SELECT hour(uber.dt) as hr,count(cid) as ct FROM uber group By hour(uber.dt)").show
    
      println("Which clusters have the highest number of pickups?")
      spark.sql("SELECT COUNT(cid), cid FROM uber GROUP BY cid ORDER BY COUNT(cid) DESC").show
 
      println("Show the  windows of latest hourly counts by cluster")
      val countsDF = df.groupBy($"cid", window($"dt", "1 hour")).count()
      countsDF.createOrReplaceTempView("uber_counts")
      */



  }
}

