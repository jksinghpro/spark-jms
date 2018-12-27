package jk.bigdata.tech.jms

import java.util.Collections
import javax.jms._

import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.beans.BeanProperty
import scala.collection.mutable.ListBuffer

/**
  * Created by exa00015 on 17/12/18.
  */
class JmsDatasourceRelation(override val sqlContext: SQLContext, parameters: Map[String, String]) extends BaseRelation with TableScan with Serializable {

  lazy val RECIEVER_TIMEOUT = parameters.getOrElse("reciever.timeout","3000").toLong


  override def schema: StructType = {
    ScalaReflection.schemaFor[JmsMessage].dataType.asInstanceOf[StructType]
  }

  override def buildScan(): RDD[Row] = {
    val connection = DefaultSource.connectionFactory(parameters).createConnection
    val session: Session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
    connection.start
    var messageRdd:RDD[Row] = null
    try {
      val queue = session.createQueue(parameters("queue"))
      val consumer = session.createConsumer(queue)
      var break = true
      val messageList :ListBuffer[JmsMessage] = ListBuffer()
      while (break) {
        val textMsg = consumer.receive(RECIEVER_TIMEOUT).asInstanceOf[TextMessage]
        /*textMsg  match {
          case msg:TextMessage =>
          case msg:BytesMessage => {
            var byteData:Array[Byte] = null
            byteData = new Array[Byte](msg.getBodyLength.asInstanceOf[Int])
            msg.readBytes(byteData)
          }
          case msg:ObjectMessage=> msg.getObject
          case msg:StreamMessage =>
          case msg:MapMessage =>
        }*/
        if(parameters.getOrElse("acknowledge","false").toBoolean && textMsg !=null){
          textMsg.acknowledge
        }
        textMsg match {
          case null => break = false
          case _ =>  messageList += JmsMessage(textMsg)
        }
      }
      import scala.collection.JavaConverters._
      val messageDf = sqlContext.createDataFrame(messageList.toList.asJava,classOf[JmsMessage])
      messageRdd = messageDf.rdd
    } finally {
      connection.close
    }
    messageRdd
  }

}
