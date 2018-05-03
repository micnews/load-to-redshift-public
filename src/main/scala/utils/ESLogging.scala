package utils

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import com.sksamuel.elastic4s.aws.{Aws4ElasticClient, Aws4ElasticConfig}
import com.typesafe.config.ConfigFactory

object ESLogging {

  val LOAD = "load"
  val ERROR = "error"
  val MISSING_COLUMN = "missing_column"

  val aws_id = "xxx"
  val aws_secret = "xxx"

  abstract class LogEvent
  case class LoadEvent(         timestamp: String, count: Long,     fileName: String, tableName: String, msg_type: String = LOAD) extends LogEvent
  case class ErrorEvent(        timestamp: String, error: String,   fileName: String, tableName: String, msg_type: String = ERROR) extends LogEvent
  case class MissingColumnEvent(timestamp: String, column: String,  fileName: String, tableName: String, msg_type: String = MISSING_COLUMN) extends LogEvent

  private val conf = ConfigFactory.load()
  private val esHost = conf.getString("esHost")

  val awsEsConfig = Aws4ElasticConfig(esHost, aws_id, aws_secret, "us-east-1")

  import com.sksamuel.elastic4s.http.ElasticDsl._

  def log(event: LogEvent): Unit ={

    val client = Aws4ElasticClient(awsEsConfig)

    println(
      client.execute {
        createIndex("pipeline").mappings(
          mapping("etl").fields(
            keywordField("msg_type"),
            dateField("timestamp"),
            longField("count"),
            keywordField("error"),
            keywordField("column"),
            textField("fileName"),
            keywordField("tableName")
          )
        )
      }.await
    )

    import io.circe.generic.auto._
    import com.sksamuel.elastic4s.circe._

    event match {
      case le: LoadEvent =>           println(client.execute(indexInto("pipeline" / "etl").doc(le)).await)
      case ee: ErrorEvent =>          println(client.execute(indexInto("pipeline" / "etl").doc(ee)).await)
      case mce: MissingColumnEvent => println(client.execute(indexInto("pipeline" / "etl").doc(mce)).await)
    }

    client.close()
  }

  def currentTimeString(): String ={
    val tz = TimeZone.getTimeZone("UTC")
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'")
    df.setTimeZone(tz)
    df.format(new Date())
  }

  def main (args: Array[String]): Unit ={
    log(LoadEvent(currentTimeString(), 10, "s3a://test", "test_table"))
    log(ErrorEvent(currentTimeString(), "error message", "s3a://test", "test_table"))
    log(MissingColumnEvent(currentTimeString(), "column_a", "s3a://test", "test_table"))
  }

}
