package com.github.andr83.parsek.spark.sink

import java.io.{File, FileWriter}
import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.UUID

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import com.github.andr83.parsek._
import com.github.andr83.parsek.formatter.FieldFormatter
import com.github.andr83.parsek.serde.{SerDe, Serializer, StringSerializer}
import com.github.andr83.parsek.spark.ParsekJob
import com.github.andr83.parsek.spark.sink.FileChannelSink.FileWriterActor.{FilePayload, Ok, Tick}
import com.github.andr83.parsek.spark.util.RDDUtils
import com.github.andr83.parsek.spark.util.RDDUtils.{DefaultPartitioner, FieldsPartitioner}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

//import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Asynchronous local file channel sync with file size rolling and partition support.
  *
  * @param path            Path to output results
  * @param serializer      factory function which return Serializer instance
  * @param partitions      FieldFormatter's which will define partition rules
  * @param fileNamePattern pattern for output files. Default is partition values joined with underscore ('_')
  * @param rollSize        file size limit to roll it after. Current file will be renamed to file.N where N is next number available.
  * @author andr83
  */
case class FileChannelSink(
  path: String,
  serializer: () => Serializer = StringSerializer.factory,
  partitions: Seq[FieldFormatter] = Seq.empty[FieldFormatter],
  fileNamePattern: Option[String] = None,
  numPartitions: Option[Int] = None,
  rollSize: Long = 0
) extends Sink {

  import FileChannelSink._

  def this(config: Config) = this(
    path = config.as[String]("path"),
    serializer = config.as[Option[Config]]("serializer")
      .map(serializerConf => () => SerDe(serializerConf))
      .getOrElse(StringSerializer.factory),
    partitions = if (config.hasPath("partitions"))
      FieldFormatter(config.getList("partitions"))
    else Seq.empty[FieldFormatter],
    fileNamePattern = config.as[Option[String]]("fileNamePattern"),
    numPartitions = config.as[Option[Int]]("numPartitions"),
    rollSize = config.as[Option[Long]]("rollSize").getOrElse(0L)
  )

  lazy val fileManagerActor = ParsekJob.getActor("FileChannelSink.fileManagerActor", Props(new FileManagerActor(path, rollSize)))

  override def sink(rdd: RDD[PValue], time: Long): Unit = {
    if (rdd.isEmpty()) {
      logger.info("Rdd is empty")
    } else {
      new java.io.File(path).mkdirs()

      val pattern = fileNamePattern map (pattern => {
        pattern
          .replaceAllLiterally("${randomUUID}", UUID.randomUUID().toString)
          .replaceAllLiterally("${timeInMs}", time.toString)
      })

      val partitioner = if (partitions.isEmpty) DefaultPartitioner(pattern)
      else FieldsPartitioner(partitions, pattern)

      RDDUtils
        .serializeAndPartitionBy(rdd, serializer, partitioner, numPartitions)
        .aggregateByKey(Seq.empty[String])(_ :+ _, _ ++ _)
        .foreach {
          case (fileName, lines) =>
            fileManagerActor ! Payload(fileName, lines)
        }
    }
  }
}

object FileChannelSink {

  case class Payload(fileName: String, lines: Seq[String])

  class FileManagerActor(baseDirectory: String, rollSize: Long) extends Actor {
    var writerActors = Map.empty[String, ActorRef]

    override val supervisorStrategy =
      OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
        case _: Exception => Stop
      }

    def receive = {
      case Payload(fileName, lines) => getFileWriter(fileName) ! FilePayload(lines)
      case Terminated(child) => writerActors = writerActors filterNot (_._2 == child)
    }

    def getFileWriter(fileName: String): ActorRef = writerActors.getOrElse(fileName, {
      val writer = context.actorOf(Props(new FileWriterActor(Paths.get(baseDirectory, fileName).toString, rollSize)))
      writerActors += fileName -> writer
      writer
    })
  }

  class FileWriterActor(fileName: String, rollSize: Long) extends Actor with akka.actor.ActorLogging {
    val file = new File(fileName)
    file.getParentFile.mkdirs()

    var buffer = Seq.empty[String]

    implicit val ec = context.dispatcher

    val cancellable = context.system.scheduler.schedule(10.seconds, 10.seconds, self, Tick)

    override def postStop(): Unit = {
      cancellable.cancel()
    }

    def ready(): Receive = {
      val inactiveFrom = LocalDateTime.now()

      PartialFunction {
        case FilePayload(lines) =>
          context become storing
          writeLines(lines)
        case Tick if ChronoUnit.HOURS.between(inactiveFrom, LocalDateTime.now()) >= 1 =>
          self ! PoisonPill
        case _ =>
      }
    }

    def storing: Receive = {
      case FilePayload(lines) => buffer = buffer ++ lines
      case Ok if buffer.nonEmpty =>
        writeLines(buffer)
        buffer = Seq.empty[String]
      case Ok => context become ready()
      case _ =>
    }

    def receive = ready()

    def writeLines(lines: Seq[String]): Unit = {
      if (rollSize > 0) {
        var fileSize: Long = if (file.exists()) {
          file.length()
        } else 0L

        val buffer = new ArrayBuffer[Byte]()
        for (line <- lines) {
          val bLine = line.getBytes()
          if (fileSize + buffer.size + bLine.size >= rollSize) {
            if (fileSize + buffer.size >= rollSize) {
              rollFile(file)
              fileSize = 0L
            }
            if (buffer.nonEmpty) {
              dump(buffer.take(buffer.length - 1).toArray)
              fileSize += buffer.size - 1
              buffer.clear()
            }
          }

          buffer ++= bLine
          buffer += '\n'
        }

        if (buffer.nonEmpty) {
          if (fileSize + buffer.size >= rollSize) {
            rollFile(file)
          }
          dump(buffer.take(buffer.length - 1).toArray)
        }
      } else if (lines.nonEmpty) {
        val buffer = (lines.mkString("\n") + "\n").getBytes()
        dump(buffer)
      }
    }

    /**
      * Write buffer to file
      *
      * @param content Data content
      */
    def dump(content: Array[Byte]): Unit = {
      val f = Future {
        val fw = new FileWriter(fileName, true)
        try {
          fw.write(content.asStr)
          log.info(s"Write ${content.length} bytes to $fileName")
        } finally fw.close()
      }

      f onSuccess {
        case _ => self ! Ok
      }

      f onFailure {
        case error: Exception =>
          log.error("Dump error:" + error.getMessage, error)
          throw error
      }
    }

    /**
      * Roll file to fileName.N
      *
      * @param file  file path to roll
      * @param index next index
      */
    def rollFile(file: File, index: Int = 0): Unit = {
      val rolledFile = new File(file.getAbsolutePath + "." + index)
      if (rolledFile.exists()) {
        rollFile(file, index + 1)
      } else {
        file.renameTo(rolledFile)
        log.info(s"Rename $file to $rolledFile")
      }
    }
  }

  object FileWriterActor {

    object Ok

    object Tick

    case class FilePayload(lines: Seq[String])

  }

}
