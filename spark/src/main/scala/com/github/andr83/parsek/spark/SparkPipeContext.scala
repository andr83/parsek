package com.github.andr83.parsek.spark

import com.github.andr83.parsek.spark.SparkPipeContext._
import com.github.andr83.parsek.{IntCounter, PipeContext}
import org.apache.spark._
import org.apache.spark.serializer.JavaSerializer

import scala.collection.mutable.{HashMap => MutableHashMap}

/**
 * @author andr83
 */
class SparkPipeContext(acc: Accumulable[MutableHashMap[StringTuple2, Int], (StringTuple2, Int)]) extends PipeContext {

  override def getCounter(groupName: String, name: String): IntCounter = {
    val key = (groupName, name)
    if (!counters.contains(key)) {
      val counter = new IntCounter() {
        override def +=(inc: Int): IntCounter = {
          acc += key -> inc
          super.+=(inc)
        }
      }
      counters += key -> counter
    }
    counters.get(key).get
  }

  override def getCounters: Map[(String, String), Int] = acc.value.toMap
}

object SparkPipeContext {
  type StringTuple2 = (String, String)

  implicit object IntCountersParam extends AccumulableParam[MutableHashMap[StringTuple2, Int], (StringTuple2, Int)] {

    def addAccumulator(acc: MutableHashMap[StringTuple2, Int], elem: (StringTuple2, Int)): MutableHashMap[StringTuple2, Int] = {
      val (k1, v1) = elem
      acc += acc.find(_._1 == k1).map {
        case (k2, v2) => k2 -> (v1 + v2)
      }.getOrElse(elem)
      acc
    }

    /*
     * This method is allowed to modify and return the first value for efficiency.
     *
     * @see org.apache.spark.GrowableAccumulableParam.addInPlace(r1: R, r2: R): R
     */
    def addInPlace(acc1: MutableHashMap[StringTuple2, Int], acc2: MutableHashMap[StringTuple2, Int]): MutableHashMap[StringTuple2, Int] = {
      acc2.foreach(elem => addAccumulator(acc1, elem))
      acc1
    }

    /*
     * @see org.apache.spark.GrowableAccumulableParam.zero(initialValue: R): R
     */
    def zero(initialValue: MutableHashMap[StringTuple2, Int]): MutableHashMap[StringTuple2, Int] = {
      val ser = new JavaSerializer(new SparkConf(false)).newInstance()
      val copy = ser.deserialize[MutableHashMap[StringTuple2, Int]](ser.serialize(initialValue))
      copy.clear()
      copy
    }
  }

  def apply(sc: SparkContext): SparkPipeContext = 
    new SparkPipeContext(sc.accumulable(MutableHashMap.empty[StringTuple2, Int])(IntCountersParam))
}