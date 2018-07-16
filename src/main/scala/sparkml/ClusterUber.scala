package sparkml

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.types.TimestampType

object ClusterUber {

  case class Uber(dt: java.sql.Timestamp, lat: Double, lon: Double, base: String) extends Serializable

  val schema = StructType(Array(
    StructField("dt", TimestampType, true),
    StructField("lat", DoubleType, true),
    StructField("lon", DoubleType, true),
    StructField("base", StringType, true)
  ))

  def main(args: Array[String]) {

    var file1: String = "/mapr/demo.mapr.com/data/uber.csv"
    var savedirectory: String = "/mapr/demo.mapr.com/data/ubermodel"
    var file2: String = "/mapr/demo.mapr.com/data/uberjson"

    if (args.length == 2) {
      file1 = args(0)
      savedirectory = args(1)
    } else {
      System.out.println("Using hard coded parameters  ")
    }

    val spark: SparkSession = SparkSession.builder().appName("uber").getOrCreate()

    import spark.implicits._

    val df: Dataset[Uber] = spark.read.option("inferSchema", "false").schema(schema).option("header", "true").csv(file1).as[Uber]
    // val df = ds.filter($"lat" > 40.699 && $"lat" < 40.720 && $"lon" > -74.747 && $"lon" < -73.969)

    df.cache
    df.show
    df.schema
    val temp = df.select(df("base")).distinct
    temp.show

    val featureCols = Array("lat", "lon")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(df)
    df2.show
    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3), 5043)

    // increase the iterations if running on a cluster (this runs on a 1 node sandbox)
    val kmeans = new KMeans().setK(10).setFeaturesCol("features").setMaxIter(10)
    val model = kmeans.fit(trainingData)
    println("Final Centers: ")
    model.clusterCenters.foreach(println)

    val clusters = model.transform(df2)
    clusters.show
    clusters.createOrReplaceTempView("uber")

    println("Which clusters had the highest number of pickups?")
    clusters.groupBy("prediction").count().orderBy(desc("count")).show
    println("select prediction, count(prediction) as count from uber group by prediction")
    spark.sql("select prediction, count(prediction) as count from uber group by prediction").show

    println("Which cluster/base combination had the highest number of pickups?")
    clusters.groupBy("prediction", "base").count().orderBy(desc("count")).show

    println("which hours of the day and which cluster had the highest number of pickups?")
    clusters.select(hour($"dt").alias("hour"), $"prediction")
      .groupBy("hour", "prediction").agg(count("prediction")
        .alias("count")).orderBy(desc("count")).show
    println("SELECT hour(uber.dt) as hr,count(prediction) as ct FROM uber group By hour(uber.dt)")
    spark.sql("SELECT hour(uber.dt) as hr,count(prediction) as ct FROM uber group By hour(uber.dt)").show

   
    
    // to save the model 
    println("save the model")
    model.write.overwrite().save(savedirectory)
    // model can be  re-loaded like this
    // val sameModel = KMeansModel.load(savedirectory)
    //  
    // to save the categories dataframe as json data
    val res = spark.sql("select dt, lat, lon, base, prediction as cid FROM uber order by dt")
    res.show
  //  res.coalesce(1).write.format("json").save(file2)

  }
}

