package com.github.andr83.parsek.spark.sink

import java.sql.Connection
import java.sql.Types._
import java.util.{Properties, ArrayList => JAList}

import com.github.andr83.parsek._
import com.typesafe.config.{Config, ConfigException}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import net.ceedubs.ficus.Ficus._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

case class HbaseSink(
  // todo phoenix.muate.batchSize ???
  tableName: String,
  phoenixConnectionUrl: String,
  phoenixConnectionProperties: Option[Properties],
  fields: Seq[String],
  duplicateKeyAction: String
) extends Sink {
  import HbaseSink._

  def this(config: Config) = this(
    config.as[String]("tableName"),
    config.as[String]("phoenix.connectionUrl"),
    config.as[Option[Map[String, String]]]("phoenix").map(m => {
      val p = new Properties()
      p.putAll(m)
      p
    }),
    config.getAnyRef("fields") match {
      case q: String => Seq(q)
      case qs: JAList[String] @unchecked => qs.toSeq
      case _ => throw new ConfigException.BadValue("fields", "Fields must be a string or list of strings")
    },
    config.as[Option[String]]("duplicateKeyAction").getOrElse("ignore")
  )

  override def sink(rdd: RDD[PValue], time: Long): Unit = {
    implicit val dsFactory = () => new HikariDataSource({
      val hikariConfig = new HikariConfig()
      hikariConfig.setJdbcUrl(phoenixConnectionUrl)
      phoenixConnectionProperties.foreach(hikariConfig.setDataSourceProperties)
      hikariConfig.setDriverClassName("org.apache.phoenix.jdbc.PhoenixDriver")
      hikariConfig
    })

    rdd.foreachPartition(it => {
      if (it.nonEmpty) {
        withConnection(conn => {
          val columnsNameType = {
            val md = conn.getMetaData.getColumns(null, null, tableName, null)
            val l = new JAList[(String, Int)](32)
            while (md.next()) {
              val columnName = md.getString("COLUMN_NAME")
              val dataType = md.getInt("DATA_TYPE")
              l.add(columnName -> dataType)
            }
            md.close()
            l
          }

          val query =
            s"""
               |upsert into $tableName (${fields.mkString(",")})
               |values (${("?"*fields.length).mkString(",")})
               |on duplicate key $duplicateKeyAction
             """.stripMargin
          val pstmt = conn.prepareStatement(query)
          conn.setAutoCommit(false)

          val datas = it.map {
            case pmap: PMap => for (key <- fields) yield key -> pmap.getValue(key)

            case x => throw new IllegalStateException(s"Expected PMap value but got $x")
          }

          for (data <- datas.map(_.toMap)) {
            for (((columnName, sqlType), _i) <- columnsNameType.zipWithIndex; i = _i + 1) {
              val value = data.get(columnName).flatten
              value match { // todo extract & TESTS
                case None =>
                  pstmt.setNull(i, sqlType)

                case Some(PInt(v)) if sqlType == BIGINT || sqlType == SMALLINT || sqlType == TINYINT || sqlType == INTEGER || sqlType == DECIMAL =>
                  pstmt.setInt(i, v)

                case Some(PLong(v)) if sqlType == BIGINT || sqlType == SMALLINT || sqlType == TINYINT || sqlType == INTEGER || sqlType == DECIMAL =>
                  pstmt.setLong(i, v)

                case Some(PDouble(v)) if sqlType == FLOAT || sqlType == DOUBLE || sqlType == REAL =>
                  pstmt.setDouble(i, v)

                case Some(PBool(v)) if sqlType == BIT =>
                  pstmt.setBoolean(i, v)

                case Some(PDate(v)) if sqlType == DATE =>
                  pstmt.setDate(i, java.sql.Date.valueOf(v.toLocalDate.toString))

                case Some(PDate(v)) if sqlType == TIME =>
                  pstmt.setTime(i, new java.sql.Time(v.toLocalTime.toDateTimeToday().getMillis))

                case Some(PDate(v)) if sqlType == TIMESTAMP =>
                  pstmt.setTimestamp(i, new java.sql.Timestamp(v.toLocalDate.toDateTimeAtStartOfDay.getMillis))

                case Some(PString(v)) if sqlType == DATE =>
                  pstmt.setDate(i, java.sql.Date.valueOf(v))

                case Some(PString(v)) if sqlType == TIME =>
                  pstmt.setTime(i, java.sql.Time.valueOf(v))

                case Some(PString(v)) if sqlType == TIMESTAMP =>
                  pstmt.setTimestamp(i, java.sql.Timestamp.valueOf(v))

                case Some(PString(v)) =>
                  pstmt.setString(i, v)

                case x =>
                  sys.error(s"Unknown type $x, with sqlType=$sqlType")
              }

            }

            pstmt.addBatch()
          }

          val result = pstmt.executeBatch()
          conn.commit()
          logger.info(s"Inserted batch of ${result.length} to $tableName")
        })
      }
    })
  }
}

object HbaseSink {
  def withConnection[A](b: Connection => A)(implicit dsFactory: () => HikariDataSource): A = {
    // todo figure out how to have datasource lazy (volatile), same with CHSink
    val conn = dsFactory().getConnection
    try {
      b(conn)
    } finally {
      conn.close()
    }
  }
}