package utils

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

import com.typesafe.config.ConfigFactory

import scala.util.{Failure, Success, Try}

object Redshift {

  val conf = ConfigFactory.load()

  val user = conf.getString("rsUsername")
  val pass = conf.getString("rsPassword")

  val rsHost = conf.getString("rsHost")

  val dbUrl = s"jdbc:redshift://$rsHost:5439/prod"

  def connect(): Try[Connection] ={
    try {
      println(">>>> Connecting to Redshift...")

      val props = new Properties()
      props.setProperty("user", user)
      props.setProperty("password", pass)

      val newConn = DriverManager.getConnection(dbUrl, props)
      println(">>>> Connected.")

      Success(newConn)
    } catch {
      case ex: Exception => {
        print(ex.toString)
        Failure(ex)
      }
    }
  }

  def executeStatement(statementString: String): Try[Unit] = {
    var stmt: Statement = null
    var connection: Try[Connection] = null

    try {
      connection = connect()
      connection match {
        case Failure(ex) => {
          print(ex.toString)
          Failure(ex)
        }
        case Success(conn) => {

          stmt = conn.createStatement

          // This enables the 'statementString' to be multi-line, so each command is executed in sequence within a session
          statementString
            .split(";")
            .map(_.trim)
            .filter(_ != "")
            .foreach(singleQuery => {
              val newStatement = singleQuery + ";"
              print(s">>>> '$newStatement'")
              stmt.execute(newStatement)
            })

          Success(Unit)
        }
      }
    } catch {
      case ex: Exception => {
        print(ex.toString)
        Failure(ex)
      }
    } finally {
      Try { if (stmt != null) stmt.close() }
      Try { connection.get.close() }
    }
  }

  def getTableColumns(conn: Connection, table: String, schema: Option[String] = None): Seq[String] ={
    val catalog           = null
    val schemaPattern     = if (schema.isDefined) schema.get else null
    val tableNamePattern  = table
    val columnNamePattern = null

    val databaseMetaData = conn.getMetaData()

    val result = databaseMetaData.getColumns(catalog, schemaPattern,  tableNamePattern, columnNamePattern)

    var columns = Seq.empty[String]

    while (result.next()){
      columns ++= Seq(result.getString(4))
    }

    columns
  }
}
