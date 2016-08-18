package com.github.andr83.parsek.spark.source

import java.sql.Timestamp
import java.util

import com.github.andr83.parsek._
import com.github.andr83.parsek.spark.SparkJob
import com.typesafe.config.{Config, ConfigException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._

/**
  * @author andr83 
  *         created on 05.08.16
  */
case class HiveSource(
  queries: Seq[String]
) extends Source {
  import HiveSource._

  def this(config: Config) = this(
    queries = config.getAnyRef("queries") match {
      case q: String => Seq(q)
      case qs: util.ArrayList[_] => qs.map(_.toString).toSeq
      case _ => throw new ConfigException.BadValue("query", "Query must be a string or list of strings")
    }
  )

  def apply(job: SparkJob): RDD[PValue] = {
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(job.sc)

    queries.reverse.tail.reverse foreach(q=> sqlContext.sql(q))
    val df = sqlContext.sql(queries.last)
    df.map(convert)
  }
}

object HiveSource {
  def convert(row: Row): PMap = {
    val map = row.schema.fields.flatMap (f=> {
      val  v = row.get(row.fieldIndex(f.name))
      if (v == null) {
        None
      } else {
        Some(f.name -> getAsType(v, f.dataType))
      }
    }).toMap
    PMap(map)
  }

  def getAsType(value: Any, dataType: DataType): PValue = dataType match {
    case StringType => PString(value.asInstanceOf[String])
    case IntegerType => PInt(value.asInstanceOf[Int])
    case LongType => PLong(value.asInstanceOf[Long])
    case FloatType => PDouble(value.asInstanceOf[Float])
    case DoubleType => PDouble(value.asInstanceOf[Double])
    case BooleanType => PBool(value.asInstanceOf[Boolean])
    case TimestampType => PDate(value.asInstanceOf[Timestamp].getTime)
    case StructType(fields) => convert(value.asInstanceOf[Row])
    case MapType(keyType, valueType, _) =>
      val res = value.asInstanceOf[Map[Any,Any]] map {case (k,v)=> k.toString -> getAsType(v, valueType)}
      PMap(res)
    case ArrayType(elementType, _) =>
      val res = value.asInstanceOf[Seq[Any]] map(getAsType(_, elementType))
      PList(res.toList)
  }
}