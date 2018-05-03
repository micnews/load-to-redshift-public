package transforms

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

abstract class Transformer {

  def transform(df: DataFrame): DataFrame

  val arrayToJsonString = udf((a: Seq[Any]) => {
    if (a == null || a.isEmpty){
      "[]"
    } else {
      "[" + a.map(i => "\"" + i + "\"").mkString(",") + "]"
    }
  })

}
