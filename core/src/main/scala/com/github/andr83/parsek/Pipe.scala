package com.github.andr83.parsek

import com.github.andr83.parsek.ParsePipe.ParseNode
import com.github.andr83.parsek.PValueImplicits._


/**
 * @author andr83
 */
sealed trait Pipe

class PipeContext {
  var state = Map.empty[String, PValue]

  def getValue(path: String): Option[PValue] = ???
}

trait SourcePipe extends Pipe with Iterable[PValue]

trait SinkPipe extends Pipe {
  def run(): Unit
}

final class ParsePipe(root: ParseNode, implicit val context: PipeContext) extends Pipe {
  def parse(source: PValue): Option[PValue] = {
    try {
      Some(parseValue(source, root))
    } catch {
      case ex: Exception => None
    }
  }

  private def parseValue(value: PValue, node: ParseNode): PValue = {
    val result = node.parser.parse(value)
    if (node.childs.nonEmpty) result match {
      case PMap(map) => node.childs.mapValues(parseNodeChilds(map, _))

      case PList(list: List[_]) => list.map {
        case item: PMap => PMap(node.childs.mapValues(parseNodeChilds(item, _)))
        case item =>  throw new IllegalStateException(s"Expected PMap value but given $item.")
      }

      case PList(list) if node.childs.size == 1 =>
        val (fieldName, child) = node.childs.head
        Map(fieldName -> PList(list.map(item => parseValue(item, child))))

      case _ => throw new IllegalStateException(s"Illegal result on parsing value $value.")
    } else {
      result
    }
  }

  private def parseNodeChilds(map: PMap, node: ParseNode): PMap = PMap(node.childs.map {
    case (fieldName, child) if map.value.contains(fieldName) =>
      fieldName -> parseValue(map.value.get(fieldName).get, child)
    case (fieldName, _) if node.isRequired =>
      throw new IllegalStateException(s"Required field $fieldName is missing.")
  })
}

object ParsePipe {

  trait Parser {
    implicit val context: PipeContext

    def parse(source: PValue): PValue
  }

  final case class ParseNode(
    parser: Parser,
    childs: Map[String, ParseNode] = Map.empty[String, ParseNode],
    isRequired: Boolean = false)

  def apply(parser: Parser)(implicit context: PipeContext) = new ParsePipe(ParseNode(parser), context)
}

class ProjectPipe extends Pipe
