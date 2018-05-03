package transforms

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import utils.LoadFunctions._

object Odin extends Transformer {

  override def transform(tempDf: DataFrame): DataFrame = {
    var df = tempDf

    df = flattenDf(df)

    for (field <- df.schema){
      if (field.dataType.typeName == "array"){
        df = df.withColumn(field.name, arrayToJsonString(col(field.name)))
      }
    }

    if (df.schema.toList.map(_.name).contains("keen__timestamp")){
      df = df.withColumn("timestamp", unix_timestamp(col("keen__timestamp"), "y-MM-dd'T'hh:mm:ss.SSS'Z'").cast("timestamp"))
      df = df.drop("keen__created_at", "keen__id", "keen__timestamp")
    }

    val naFunctions = df.na
    df = naFunctions.fill("")

    if (df.schema.toList.map(_.name).contains("active_ab_tests__mock-test")){
      df = df.drop("active_ab_tests__mock-test")
    }

    df
  }

}