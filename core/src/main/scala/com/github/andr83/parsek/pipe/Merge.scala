package com.github.andr83.parsek.pipe

import com.github.andr83.parsek._
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

/**
  * Merge multiple PMap fields on the same level to one PMap and store as "as" field
  * If as field is not defined result PMap will store on parent level.
  *
  * @param fields list of fields to merge
  *
  * @author andr83
 */
case class Merge(fields: Seq[FieldPath], as: Option[String] = None) extends Pipe {

  def this(config: Config) = this(
    fields = config.as[List[String]]("fields").map(_.asFieldPath),
    as = config.as[Option[String]]("as")
  )

  val (root: FieldPath, fs: Seq[String]) = {
    val root = fields.head.take(fields.head.length - 1)
    val fs = for{
      f <- fields
      path = f.take(f.length - 1)
    } yield if (path == root) f.last else throw new IllegalStateException(s"Merged can be only fields on the same level")
    root -> fs
  }
  
  override def run(value: PValue)(implicit context: PipeContext): Option[PValue] = merge(value, root)

  def merge(value: PValue, in_path: Seq[String]): Option[PValue] = value match {
    case PMap(map) if in_path.nonEmpty =>
      for{
        v  <- map.get(in_path.head)
        mv <- merge(v, in_path.tail)
      } yield map + (in_path.head -> mv)
    case PMap(map) =>
      val res: Map[String, PValue] = (for {
        f <- fs
        v <- map.get(f)
      } yield v match {
        case PMap(m) => m
        case _ => throw new IllegalStateException(s"Value in ${(root+f).mkString(".")} must be a PMap")
      }).reduce(_ ++ _)

      as match {
        case Some(asField) => Some(PMap(map + (asField -> PMap(res))))
        case None => Some(PMap(res))
      }
    case PList(list) =>
      val res = list.flatMap(merge(_, in_path))
      if (res.isEmpty) None else Some(PList(res))
    case v => throw new IllegalStateException(s"Can not get value in path ${root.mkString(".")} for merge")
  }
}
