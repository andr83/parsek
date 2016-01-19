package com.github.andr83.parsek.spark.sink

import java.util

import com.github.andr83.parsek._
import com.github.andr83.parsek.meta._
import com.github.andr83.parsek.serde.JsonSerDe
import com.typesafe.config.{Config, ConfigException}
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

/**
  * Convert RDD to Json DataFrame and execute hive query on it.
  * DataFrame registered as temp table "sink_table" in hive
  *
  * @param queries Hive query list to execute
  * @param fields sink_table schema
  * @param numPartitions count of result partitions
  *
  * @author andr83
  */
case class HiveSink(
  queries: Seq[String],
  fields: List[FieldType],
  numPartitions: Int = 0
) extends Sink {
  import HiveSink._

  def this(config: Config) = this(
    queries = config.getAnyRef("query") match {
      case q: String => Seq(q)
      case qs: util.ArrayList[_] => qs.map(_.toString).toSeq
      case _ => throw new ConfigException.BadValue("query", "Query must be a string or list of strings")
    },
    fields = config.as[List[Config]]("fields") map Field.apply,
    numPartitions = config.as[Option[Int]]("numPartitions").getOrElse(0)
  )

  val schema = createSchema(fields)

  override def sink(rdd: RDD[PValue]): Unit = {
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(rdd.context)

    var jsonRdd = rdd.mapPartitions(it => {
      val ser = JsonSerDe()
      it.map(v => new String(ser.write(v).map(_.toChar)))
    })

    if (numPartitions > 0) {
      jsonRdd = jsonRdd.repartition(numPartitions)
    }

    val df = sqlContext.jsonRDD(jsonRdd, schema = schema)
    df.registerTempTable("sink_table")

    val fieldNames = fields map(_.asField) mkString ","
    queries foreach (query=> {
      val queryWithFields = query.replaceAllLiterally("${fields}", fieldNames)
      logger.debug(s"Executing query: $queryWithFields")
      sqlContext.sql(queryWithFields)
    })
  }
}

object HiveSink {
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
}