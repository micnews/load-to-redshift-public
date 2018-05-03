package transforms

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import utils.LoadFunctions._

object Sendgrid extends Transformer {

  override def transform(tempDf: DataFrame): DataFrame = {
    var df = tempDf

    df = flattenDf(df)

    for (field <- df.schema){
      if (field.dataType.typeName == "array"){
        df = df.withColumn(field.name, arrayToJsonString(col(field.name)))
      }
    }

    df = df.withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))

    df = df.drop("smtp-id")

    df
  }

}