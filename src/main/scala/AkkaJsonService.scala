import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.server.Directives._

import spray.json._
import DefaultJsonProtocol._ 

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

import scala.collection.mutable.HashMap
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import java.util.Date
import scala.concurrent.duration._

object MetricObjectValues {
  def getTheMetricValue(fixedMetricType:String) = {
    if(fixedMetricType == 0){
      Random.nextInt(100)
    }
    else if(fixedMetricType == 1){
      Random.nextInt(64)
    }
    else if(fixedMetricType == 2){
      Random.nextInt(500)
    }
    else if(fixedMetricType == 3){
      Random.nextInt(1000)
    }
    else if(fixedMetricType == 4){
      Random.nextInt(2000)
    }
    else if(fixedMetricType == 5){
      Random.nextInt(100)
    }
    else{
      Random.nextInt(10)
    }
  }

}

case class Metric(metricName: String, value: Int, timestamp: Date, host: String, region: String)

object JsonFormats {
  import DefaultJsonProtocol._

  implicit object DateJsonFormat extends JsonFormat[Date] {
    def write(date: Date): JsValue = JsString(date.getTime.toString)
    def read(json: JsValue): Date = json match {
      case JsString(str) => new Date(str.toLong)
      case _ => deserializationError("Expected date in String format")
    }
  }

  implicit val metricFormat: RootJsonFormat[Metric] = jsonFormat5(Metric.apply)
}

import JsonFormats._
object AkkaJsonService extends App {  
  implicit val system = ActorSystem(Behaviors.empty, "AkkaKafkaProducer")
  implicit val executionContext = system.executionContext

  val producerProps = new Properties()
  producerProps.put("bootstrap.servers", "localhost:9092")
  producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](producerProps)

  val hosts = ArrayBuffer[String]("server01","server02","server03","server04","server05")

  val regions = ArrayBuffer[String]("us-east-1","us-west-2","eu-west-1","eu-central-1","ap-south-1")

  val metricTypes = HashMap[Int, String](
    0 -> "cpu_usage_percentage",
    1 -> "memory_usage_gb",
    2 -> "disk_io_rate_mbps",
    3 -> "network_throughput_mbps",
    4 -> "response_time_ms",
    5 -> "error_rate_percentage"
  )
  
  system.scheduler.scheduleWithFixedDelay(initialDelay = 0.seconds, delay = 8.seconds) { () =>
    val fixedMetricId = metricTypes.keys.toSeq(Random.nextInt(metricTypes.size-1))
    val fixedMetricType = metricTypes(fixedMetricId)
    val fixedMetricValue = MetricObjectValues.getTheMetricValue(fixedMetricType)
    val fixedHost = hosts(Random.nextInt(hosts.length -1 ))
    val fixedRegion = regions(Random.nextInt(regions.length-1))

    val jsonobj = Metric(fixedMetricType,fixedMetricValue,new Date(),fixedHost,fixedRegion)
    println(jsonobj)

    val jsonString = jsonobj.toJson.toString()
    val record = new ProducerRecord[String, String]("metricmessage",jsonString)
    producer.send(record)

  }

}
