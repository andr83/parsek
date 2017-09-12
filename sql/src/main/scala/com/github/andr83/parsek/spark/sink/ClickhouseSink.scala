package com.github.andr83.parsek.spark.sink

import java.sql.{Connection, ResultSet, Types}
import java.util.{Properties, ArrayList => JAList}

import com.github.andr83.parsek._
import com.github.andr83.parsek.formatter.DateFormatter
import com.github.andr83.parsek.meta._
import com.github.andr83.parsek.serde.StringSerializer
import com.typesafe.config.{Config, ConfigException}
import net.ceedubs.ficus.Ficus._
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import ru.yandex.clickhouse.util.TypeUtils
import ru.yandex.clickhouse.{ClickHouseDataSource, ClickHouseUtil}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * @author andr83 
  *         created on 09.08.16
  */
case class ClickhouseSink(
  jdbcUrl: String,
  tableName: String,
  fields: Seq[String]
) extends Sink {
  import ClickhouseSink._

  def this(config: Config) = this(
    jdbcUrl = config.as[String]("jdbcUrl"),
    tableName = config.as[String]("tableName"),
    fields = config.getAnyRef("fields") match {
      case q: String => Seq(q)
      case qs: JAList[String] @unchecked => qs.toSeq
      case _ => throw new ConfigException.BadValue("fields", "Fields must be a string or list of strings")
    }
  )

  override def sink(rdd: RDD[PValue], time: Long): Unit = {
    implicit val dsFactory = () => new ClickHouseDataSource(jdbcUrl, new Properties())
    rdd.foreachPartition(it => {
      if (it.nonEmpty) {
        withConnection(conn => {
          val tableSchema: RecordField = withConnection(conn => clickhouseTableDescription(conn, tableName))
          val fieldsToSave = tableSchema.fields.filter(f => fields.contains(f.asField.replace(".", "\\.")))

          val queryStart = s"INSERT INTO $tableName (${fieldsToSave.map(_.name).mkString(",")}) VALUES "
          val query = new mutable.StringBuilder(queryStart)
          var prefix = ""

          query.append(prefix)
          prefix = ","
          val rowValues = it.map {
            case v: PMap => strValues(v, fieldsToSave) mkString("(", ",", ")")
            case x => throw new IllegalStateException(s"Expected PMap value but got $x")
          }.toList
          query.append(rowValues.mkString(","))
          logger.info(rowValues.head)
          logger.info(s"Storing ${rowValues.size} rows.")
          //logger.info(query.toString())
          conn.createStatement().execute(query.toString())
        })
      }
    })
  }
}

object ClickhouseSink {
  lazy val stringSerializer = StringSerializer.factory()

  private[this] val numberStringify = PartialFunction[PValue, String] {
    case PInt(n) => n.toString
    case PLong(n) => n.toString
    case PDouble(n) => n.toString
    case PBool(b) => if (b) "1" else "0"
    case x => throw new IllegalStateException(s"Expected number value got $x")
  }

  def withConnection[A](block: Connection => A)(implicit dsFactory: () => ClickHouseDataSource): A = {
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
    val conn = dsFactory().getConnection
    try {
      block(conn)
    } finally {
      conn.close()
    }
  }

  private val arrayTypeRegexp = "Array\\((\\w+)\\)".r

  def clickhouseTableDescription(connection: Connection, tableName: String): RecordField =
    RecordField(name = "root", fields = {
      val fields = new Iterator[FieldType] {
        val (db, table) = if (tableName.contains(".")) {
          val of: Int = tableName.indexOf(".")
          tableName.substring(0, of) -> tableName.substring(of + 1)
        } else ("", tableName)
        private val columns: ResultSet = connection.createStatement().executeQuery(
          s"select name, type " +
            s"from system.columns " +
            s"where " +
            s"  concat(database,'.',table) = '$tableName' " +
            s"  and default_kind <> 'MATERIALIZED'")

        override def hasNext: Boolean = columns.next()

        def sqlTypeToParseField(name: String, sqlType: String): FieldType = {
          TypeUtils.toSqlType(sqlType) match {
            case Types.BIGINT => LongField(name)
            case Types.INTEGER => IntField(name)
            case Types.VARCHAR => StringField(name)
            case Types.FLOAT => DoubleField(name)
            case Types.DOUBLE => DoubleField(name)
            case Types.DATE => DateField(name, pattern = DateFormatter(Some("yyyy-MM-dd")))
            case Types.TIMESTAMP => DateField(name, pattern = DateFormatter(Some("yyyy-MM-dd HH:mm:ss")))
            case Types.ARRAY => {
              val innerType = sqlType match {
                case arrayTypeRegexp(innerTypeStr) => sqlTypeToParseField(name, innerTypeStr)
              }
              ListField(name, Some(innerType))
            }
            case x => throw new IllegalStateException(s"Unsupported type $x on field $name")
          }
        }

        override def next(): FieldType = {
          val name = columns.getString(1)
          val sqlType = columns.getString(2)
          sqlTypeToParseField(name, sqlType)
        }
      }.toList
      fields
    })

  def strValues(row: PMap, fields: Seq[FieldType]): Seq[String] = fields.map(f => {
    row.getValue(f.asField).map(stringify(_, f)).getOrElse {
      f match {
        case _: IntField => "0"
        case _: LongField => "0"
        case _: DoubleField => "0"
        case _: BooleanField => "0"
        case _ => "'NULL'"
      }
    }
  })

  def quote(str: String) = "'" + ClickHouseUtil.escape(str) + "'"

  private val defaultField = StringField("")

  def stringify(value: PValue, field: FieldType): String = field match {
    case f: BooleanField =>
      value match {
        case PBool(b) => if (b) "1" else "0"
        case PInt(n) if n >= 0 && n <= 1 => n.toString
        case PLong(n) if n >= 0 && n <= 1 => n.toString
        case x => throw new IllegalStateException(s"Expected boolean value for field ${field.asField} got $x")
      }
    case f: DateField =>
      value match {
        case PDate(d) => quote(f.pattern.format(d).value.toString)
        case PLong(n) => quote(f.pattern.format(new DateTime(n)).value.toString)
        case x => throw new IllegalStateException(s"Expected date value for field ${field.asField} got $x")
      }
    case f: RecordField =>
      value match {
        case m: PMap => "[" + strValues(m, f.fields).mkString(",") + "]"
        case x => throw new IllegalStateException(s"Expected Record value for field ${field.asField} got $x")
      }
    case f: ListField =>
      value match {
        case PList(list) => "[" + list.map(v => stringify(v, f.field.getOrElse(defaultField))).mkString(",") + "]"
        case x => throw new IllegalStateException(s"Expected List value for field ${field.asField} got $x")
      }
    case f: IntField => numberStringify(value)
    case f: LongField => numberStringify(value)
    case f: DoubleField => numberStringify(value)
    case _ => quote(stringSerializer.write(value).asStr)
  }
}
