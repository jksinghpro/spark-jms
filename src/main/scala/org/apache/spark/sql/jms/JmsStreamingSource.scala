package org.apache.spark.sql.jms

import javax.jms._

import jk.bigdata.tech.jms.{DefaultSource, JmsMessage, JmsSourceOffset}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by exa00015 on 25/12/18.
  */

class JmsStreamingSource(sqlContext: SQLContext,
                         parameters: Map[String, String],
                         metadataPath: String,
                         failOnDataLoss: Boolean
                        ) extends Source {

  var counter = sqlContext.sparkContext.longAccumulator("counter")

  lazy val RECIEVER_TIMEOUT = parameters.getOrElse("reciever.timeout", "3000").toLong


  val connection = DefaultSource.connectionFactory(parameters).createConnection
  val session: Session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
  connection.start

  override def schema: StructType = {
    ScalaReflection.schemaFor[JmsMessage].dataType.asInstanceOf[StructType]
  }

  override def getOffset: Option[Offset] = {
    counter.add(1)
    Some(JmsSourceOffset(counter.value))
  }


  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val queue = session.createQueue(parameters("queue"))
    val consumer = session.createConsumer(queue)
    var break = true
    val messageList: ListBuffer[JmsMessage] = ListBuffer()
    while (break) {
      val textMsg = consumer.receive(RECIEVER_TIMEOUT).asInstanceOf[TextMessage]
      if (parameters.getOrElse("acknowledge", "false").toBoolean && textMsg != null) {
        textMsg.acknowledge
      }
      textMsg match {
        case null => break = false
        case _ => messageList += JmsMessage(textMsg)
      }
    }
    import org.apache.spark.unsafe.types.UTF8String._
    val internalRDD = messageList.map(message => InternalRow(
      fromString(message.content),
      fromString(message.correlationId),
      fromString(message.jmsType),
      fromString(message.messageId),
      fromString(message.queue)
    ))
    val rdd = sqlContext.sparkContext.parallelize(internalRDD)
    sqlContext.internalCreateDataFrame(rdd,schema,true)
  }

  override def stop(): Unit = {
    session.close
    connection.close
  }

}

