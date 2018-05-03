package utils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

object LoadFunctions {

  private def flattenSchema(schema: StructType, prefix: String = null): Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName).alias(colName.replaceAll("\\.", "__")))
      }
    })
  }

  private def flattenSchema_noPrefix(schema: StructType, prefix: String = null): Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema_noPrefix(st, colName)
        case _ => Array(col(colName).alias(colName.split("\\.").last))
      }
    })
  }

  def flattenDf(df: DataFrame): DataFrame = {
    println("flattenDf")
    df.select(flattenSchema(df.schema): _*)
  }

  def flattenDf_noPrefix(df: DataFrame): DataFrame = {
    println("flattenDf_noPrefix")
    df.select(flattenSchema_noPrefix(df.schema): _*)
  }

  def setNullableStateForAllColumns( df: DataFrame, nullable: Boolean) : DataFrame = {
    // get schema
    val schema = df.schema
    // modify [[StructField] with name `cn`
    val newSchema = StructType(schema.map {
      case StructField( c, t, _, m) ⇒ StructField( c, t, nullable = nullable, m)
    })
    // apply new schema
    df.sqlContext.createDataFrame( df.rdd, newSchema )
  }

  def writeDfToRedshift(df: DataFrame,
                        database: String,
                        schema: String,
                        table: String,
                        username: String,
                        password: String,
                        host: String,
                        tempS3Dir: String,
                        iam: String
                       ): Unit = {
    println("writeDfToRedshift")

    df.write
      .format("com.databricks.spark.redshift")
      .option("url", s"jdbc:redshift://$host:5439/$database?user=$username&password=$password")
      .option("dbtable", s"$schema.$table")
      .option("tempdir", tempS3Dir)
      .option("aws_iam_role", iam)
      .mode("append")
      .save()
  }

}
