package transforms

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import utils.LoadFunctions._

object AppleNews extends Transformer {

  val sample = "/Users/jcorey/Downloads/an-events-prod-2-2018-03-30-19-42-05-b781c0d0-980e-40df-b0d7-3ba9f5247e1d"

  def main (args: Array[String]): Unit ={
    val spark = SparkSession
      .builder()
      .appName("LoadToRedshift")
      .master("local[*]")
      .getOrCreate()

    var df = spark.read.json(sample)

    df = transform(df)

    df.show(10)
  }

  override def transform(tempDf: DataFrame): DataFrame = {
    var df = tempDf

    df = df.withColumn("events", explode(df("events")))
    df = df.withColumn("article_id", regexp_extract(df("events.articleId"), "articles/([0-9]*)/", 1))
    df = df.withColumn("video_id", regexp_extract(df("events.articleId"), "previews/(.*)", 1))

    df = flattenDf_noPrefix(df)

    df = df.withColumn("authors", arrayToJsonString(col("authors")))
    df = df.withColumn("sections", arrayToJsonString(col("sections")))

    df = df.withColumn("timestamp", unix_timestamp(col("openedat"), "y-MM-dd'T'hh:mm:ss'Z'").cast("timestamp"))

    df
  }

}