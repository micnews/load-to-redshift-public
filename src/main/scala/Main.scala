import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import transforms.{AppleNews, Transformer}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import utils.ESLogging.{ErrorEvent, LoadEvent, LogEvent, MissingColumnEvent}
import utils.LoadFunctions._
import utils.{ESLogging, Redshift, SQS}

object Main {

  val conf = ConfigFactory.load()

  val rsUsername = conf.getString("rsUsername")
  val rsPassword = conf.getString("rsPassword")

  val rsHost = conf.getString("rsHost")
  val tempS3Dir = conf.getString("tempS3Dir")

  val iamRole = conf.getString("redshift.iam")

  val charLength = 8192

  val spark = SparkSession
    .builder()
    .appName("LoadToRedshift")
    .master("local[*]")
    .getOrCreate()


  def main(args: Array[String]): Unit = {
    val s3File = args(0)

    if (s3File == "REPLAY_ERRORS"){
      replayErrors()
    } else {
      processFile(s3File)
    }
  }

  def replayErrors(): Unit ={

    var erroredFiles = Seq.empty[String]

    var hasNext = true

    while (hasNext){
      val tempFiles = SQS.pullAllFromQueue()
      erroredFiles ++= tempFiles
      if (tempFiles.isEmpty) hasNext = false
    }

    for (file <- erroredFiles){
      processFile(file)
    }
  }

  def processFile(s3File: String){

    println(s3File)

    val rsConn = Redshift.connect().get

    import PipelineConfig.allEtlConfigs

    for (etlConfig <- allEtlConfigs){
      if (s3File.contains(etlConfig.dir)){

        var processed: DataFrame = null

        try {

          println("s3File.contains(etlConfig.dir)")

          val df =
            if (etlConfig.format == "csv"){
              spark.read.csv(s3File)
            } else if (etlConfig.format == "json"){
              spark.read.json(s3File)
            } else { null }

          processed =
            if (etlConfig.transformers.nonEmpty){
              etlConfig.transformers.foldLeft(df)((tempDf, transformer) => {
                transformer.transform(tempDf)
              })
            } else {
              df
            }

          val existingColumns = Redshift.getTableColumns(rsConn, etlConfig.redshiftTable, Some(etlConfig.redshiftSchema)).toSet[String].map(x => x.toLowerCase)
          val processedColumns = processed.schema.toList.map(sf => (sf.name.toLowerCase, sf.dataType)).toMap

          val missingColumns = processedColumns.keySet -- existingColumns

          if (missingColumns.nonEmpty && existingColumns.nonEmpty){

            println(etlConfig)

            processed.select(missingColumns.map(col).toSeq: _*).groupBy(missingColumns.head).count().show(20)

            for (cName <- missingColumns) {
              val cType = processedColumns(cName) match {
                case IntegerType => "INTEGER"
                case LongType => "BIGINT"
                case DoubleType => "DOUBLE PRECISION"
                case FloatType => "REAL"
                case ShortType => "INTEGER"
                case ByteType => "SMALLINT" // Redshift does not support the BYTE type.
                case BooleanType => "BOOLEAN"
                case StringType => s"VARCHAR(${charLength})" // "TEXT"
                case TimestampType => "TIMESTAMP"
                case DateType => "DATE"
                case t: DecimalType => s"DECIMAL(${t.precision},${t.scale})"
                case _ => throw new IllegalArgumentException(s"Don't know how to save $cName to JDBC")
              }

              val schemaTable = s"${etlConfig.redshiftSchema}.${etlConfig.redshiftTable}"

              Redshift.executeStatement(
                s"ALTER TABLE $schemaTable ADD COLUMN $cName $cType;"
              )

              ESLogging.log(MissingColumnEvent(ESLogging.currentTimeString, cName, s3File, etlConfig.redshiftTable))
            }
          }

          processed.schema.toList.foreach(fieldStruct => {
            val colName = fieldStruct.name
            if (fieldStruct.dataType == StringType){
              val metadata = new MetadataBuilder().putLong("maxlength", charLength).build()
              processed = processed.withColumn(colName, processed(colName).as(colName, metadata))
            }
          })

          processed = setNullableStateForAllColumns(processed, true)

          writeDfToRedshift(
            processed,
            etlConfig.redshiftDatabase,
            etlConfig.redshiftSchema,
            etlConfig.redshiftTable,
            rsUsername,
            rsPassword,
            rsHost,
            tempS3Dir,
            iamRole
          )

          ESLogging.log(LoadEvent(ESLogging.currentTimeString, processed.count(), s3File, etlConfig.redshiftTable))
        } catch {
          case e: Exception => {
            SQS.addToQueue(s3File)

            println(etlConfig)
            println(e.getMessage)

            if (e.getMessage.contains("Illegal character in:")){
              processed.select(e.getMessage.split(" ").last).show(20)
            }

            ESLogging.log(ErrorEvent(ESLogging.currentTimeString, e.getMessage, s3File, etlConfig.redshiftTable))
            e.printStackTrace()
          }
        }
      }
    }

  }

}