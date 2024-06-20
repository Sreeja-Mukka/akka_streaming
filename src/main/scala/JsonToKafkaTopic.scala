import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.kafka.common.serialization.ByteArraySerializer

object JsonToKafkaTopic extends App{
  val spark = SparkSession.builder()
    .appName("KafkaProtobufJob")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "metricmessages")
    .load()

  val schema = new StructType()
    .add("host", StringType)
    .add("metricName", StringType)
    .add("region", StringType)
    .add("timestamp", StringType)
    .add("value", IntegerType)

  val metrics = df.selectExpr("CAST(value AS STRING)")
    .select(from_json($"value", schema).as("data"))
    .select("data.*")

  val protobufData = metrics.map { row =>
    import metricMessage.MetricOuterClass.Metric

    val builder = Metric.newBuilder()
    builder.setHost(row.getAs[String]("host"))
    builder.setMetricName(row.getAs[String]("metricName"))
    builder.setRegion(row.getAs[String]("region"))
    builder.setTimestamp(row.getAs[String]("timestamp"))
    builder.setValue(row.getAs[Int]("value"))
    builder.build().toByteArray
  }

  val query = protobufData
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "outputmetric")
    .option("checkpointLocation", "/Users/smukka/Desktop/kafka_spark")
    .start()

  query.awaitTermination()
}
