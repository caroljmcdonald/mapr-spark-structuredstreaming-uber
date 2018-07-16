package streaming

// http://maprdocs.mapr.com/home/Spark/Spark_IntegrateMapRStreams.html

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.streaming._

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeansModel

import com.mapr.db.spark.impl._
import com.mapr.db.spark.streaming._
import com.mapr.db.spark.sql._
import com.mapr.db.spark.streaming.MapRDBSourceConfig

/**
 * Consumes messages from a topic in MapR Streams using the Kafka interface,
 * enriches the message with  the k-means model cluster id and publishs the result in json format
 * to another topic
 * Usage: SparkKafkaConsumerProducer  <model> <topicssubscribe> <topicspublish>
 *
 *   <model>  is the path to the saved model
 *   <topics> is a  topic to consume from
 *   <topicp> is a  topic to publish to
 * Example:
 *    $  spark-submit --class com.sparkkafka.uber.SparkKafkaConsumerProducer --master local[2] \
 * mapr-sparkml-streaming-uber-1.0.jar /user/user01/data/savemodel  /user/user01/stream:ubers /user/user01/stream:uberp
 *
 *    for more information
 *    http://maprdocs.mapr.com/home/Spark/Spark_IntegrateMapRStreams_Consume.html
 */

object StructuredStreamingConsumer extends Serializable {

  case class Uber(dt: String, lat: Double, lon: Double, base: String, rdt: String) extends Serializable
  // Parse string into Uber case class
  def parseUber(str: String): Uber = {
    val p = str.split(",")
    Uber(p(0), p(1).toDouble, p(2).toDouble, p(3), p(4))
  }
  case class UberC(dt: java.sql.Timestamp, lat: Double, lon: Double,
    base: String, rdt: String,
    cid: Integer, clat: Double, clon: Double) extends Serializable

  case class Center(cid: Integer, clat: Double, clon: Double) extends Serializable

  // Uber with unique Id and Cluster id and  cluster lat lon
  case class UberwId(_id: String, dt: java.sql.Timestamp, lat: Double, lon: Double,
    base: String, cid: Integer, clat: Double, clon: Double) extends Serializable

  // enrich with unique id for Mapr-DB
  def createUberwId(uber: UberC): UberwId = {
    val id = uber.cid + "_" + uber.rdt
    UberwId(id, uber.dt, uber.lat, uber.lon, uber.base, uber.cid, uber.clat, uber.clon)
  }
  def main(args: Array[String]): Unit = {

    var topic: String = "/apps/uberstream:ubers"
    var tableName: String = "/apps/ubertable"
    var savedirectory: String = "/mapr/demo.mapr.com/data/ubermodel"

    if (args.length == 3) {
      topic = args(0)
      savedirectory = args(1)
      tableName = args(2)
     
    } else {
      System.out.println("Using hard coded parameters unless you specify topic model directory and table. <topic model table>   ")
    }

    val spark: SparkSession = SparkSession.builder().appName("stream").master("local[*]").getOrCreate()

    import spark.implicits._
    val model = KMeansModel.load(savedirectory)
    var ac = new Array[Center](10)
    var index: Int = 0

    model.clusterCenters.foreach(x => {
      ac(index) = Center(index, x(0), x(1));
      index += 1;
    })
    val ccdf = spark.createDataset(ac)
    ccdf.show

    println("readStream ")

    import spark.implicits._

    val df1 = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "maprdemo:9092")
      .option("subscribe", topic)
      .option("group.id", "testgroup")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", false)
      .option("maxOffsetsPerTrigger", 1000)
      .load()

    println(df1.schema)

    println("Enrich Transformm Stream")

    spark.udf.register(
      "deserialize",
      (message: String) => parseUber(message)
    )

    val df2 = df1.selectExpr("""deserialize(CAST(value as STRING)) AS message""")
      .select($"message".as[Uber])

    val featureCols = Array("lat", "lon")
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    val df3 = assembler.transform(df2)

    val clusters1 = model.transform(df3)

    val temp = clusters1.select(
      $"dt".cast(TimestampType),
      $"lat", $"lon", $"base", $"rdt", $"prediction"
        .alias("cid")
    )
    val clusters = temp.join(ccdf, Seq("cid")).as[UberC]

    val cdf: Dataset[UberwId] = clusters.map(uber => createUberwId(uber))

    println("write stream")

    val query3 = cdf.writeStream
      .format(MapRDBSourceConfig.Format)
      .option(MapRDBSourceConfig.TablePathOption, tableName)
      .option(MapRDBSourceConfig.IdFieldPathOption, "_id")
      .option(MapRDBSourceConfig.CreateTableOption, false)
      .option("checkpointLocation", "/tmp/uberdb")
      .option(MapRDBSourceConfig.BulkModeOption, true)
      .option(MapRDBSourceConfig.SampleSizeOption, 1000)

    query3.start().awaitTermination()

  }

}