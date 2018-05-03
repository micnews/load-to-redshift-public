import transforms._

/**
  * Created by jcorey on 4/5/18.
  */
object PipelineConfig {

  case class ETLPipeConfig(
                            dir: String,
                            redshiftDatabase: String,
                            redshiftSchema: String,
                            redshiftTable: String,
                            format: String,
                            transformers: Seq[Transformer]
                          )

  val database = "prod"
  val schema = "etl"

  val odinConfigs = Seq(
    "ad-requests",
    "ads",
    "video-page-impressions"
  ).map(collection => {
    ETLPipeConfig("mic-data-feeds/raw/kinesis/odin-collections/" + collection, database, schema, collection.replaceAll("-", "_"), "json", Seq(Odin))
  })

  val allEtlConfigs = Seq(
    ETLPipeConfig("mic-data-feeds/raw/api/an",              database, schema, "an",         "json", Seq(AppleNews)),
    ETLPipeConfig("mic-data-feeds/raw/api/ga",              database, schema, "ga",         "json", Seq(Odin)),
    ETLPipeConfig("mic-data-feeds/raw/kinesis/onesignal",   database, schema, "onesignal",  "json", Seq(Odin)),
    ETLPipeConfig("mic-data-feeds/raw/kinesis/sendgrid",    database, schema, "sendgrid",   "json", Seq(Sendgrid))
  ) ++ odinConfigs

}
