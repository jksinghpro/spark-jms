package jk.bigdata.tech.jms

import javax.jms.Message

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

/**
  * Created by exa00015 on 26/12/18.
  */
class JmsSourceRdd(sc:SparkContext) extends RDD[Message](sc, Nil){

  override def compute(split: Partition, context: TaskContext): Iterator[Message] = ???

  override protected def getPartitions: Array[Partition] = ???

}
