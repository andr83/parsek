package com.github.andr83.parsek.spark.sink

import java.sql.Timestamp
import java.util.{ArrayList => JAList}

import com.github.andr83.parsek._
import com.github.andr83.parsek.meta._
import com.github.andr83.parsek.serde.JsonSerDe
import com.typesafe.config.{Config, ConfigException}
import net.ceedubs.ficus.Ficus._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._

/**
  * Convert RDD to Json DataFrame and execute hive query on it.
  * DataFrame registered as temp table "sink_table" in hive
  *
  * @param queries Hive query list to execute
  * @param fields sink_table schema
  * @author andr83
  */
case class HiveSink(
  queries: Seq[String],
  fields: List[FieldType]
) extends Sink {
  import HiveSink._

  def this(config: Config) = this(
    queries = config.getAnyRef("query") match {
      case q: String => Seq(q)
      case qs: JAList[String] @unchecked => qs.toSeq
      case _ => throw new ConfigException.BadValue("query", "Query must be a string or list of strings")
    },
    fields = config.as[List[Config]]("fields") map Field.apply
  )

  val schema = createSchema(fields)

  override def sink(rdd: RDD[PValue], time: Long): Unit = {
    //val sqlContext = new org.apache.spark.sql.hive.HiveContext(rdd.context)
    implicit val sc = rdd.context

    val root = RecordField("root", fields)
    val rowRdd = rdd.map(createRow(_, root))
    sqlContext.createDataFrame(rowRdd, schema).registerTempTable("sink_table")

    val fieldNames = fields map(_.asField) mkString ","
    queries foreach (query=> {
      val queryWithFields = query.replaceAllLiterally("${fields}", fieldNames)
      logger.debug(s"Executing query: $queryWithFields")
      sqlContext.sql(queryWithFields)
    })

    sqlContext.dropTempTable("sink_table")
  }
}

object HiveSink {

  @volatile
  private[HiveSink] var _sqlContext: SQLContext = _
  def sqlContext(implicit sc: SparkContext): SQLContext = this.synchronized {
    if (_sqlContext == null) {
      _sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    }
    _sqlContext
  }

  private def jsonSerDe = JsonSerDe.default

  def createSchema(fields: List[FieldType]): StructType = StructType(fields map (f => StructField(f.asField, getStructFieldType(f))))

  def getStructFieldType(field: FieldType): DataType = field match {
    case f: StringField => StringType
    case f: IntField => IntegerType
    case f: LongField => LongType
    case f: DoubleField => DoubleType
    case f: BooleanField => BooleanType
    case f: DateField => TimestampType
    case f: RecordField => StructType(f.fields map (f => StructField(f.asField, getStructFieldType(f))))
    case f: MapField => MapType(keyType = StringType, valueType = f.field.map(getStructFieldType).getOrElse(StringType), valueContainsNull = true)
    case f: ListField => ArrayType(f.field.map(getStructFieldType).getOrElse(StringType))
  }

  def createRow(value: PValue, root: RecordField): Row = convert(value, root).asInstanceOf[Row]

  def convert(value: PValue, field: FieldType): Any = (value, field) match {
    case (PMap(map), f: RecordField) =>
      val res = f.fields.map {
        case innerField => map.get(innerField.asField).map(convert(_, innerField)).orNull
      }
      Row(res:_*)
    case (PMap(map), f: MapField) =>
      val innerField = f.field.getOrElse(StringField(""))
      map.mapValues(convert(_, innerField))
    case (v: PList, f: ListField) =>
      val innerField = f.field.getOrElse(StringField(""))
      v.value.map(convert(_, innerField)).toArray
    case (v: PDate, f: DateField) => new Timestamp(v.value.getMillis)
    case (v: PList, f: StringField) => jsonSerDe.write(v).asStr
    case (v: PMap, f: StringField) => jsonSerDe.write(v).asStr
    case (v: PList, _) => null
    case (v: PMap, _) => null
    case _ => value.value
  }
}