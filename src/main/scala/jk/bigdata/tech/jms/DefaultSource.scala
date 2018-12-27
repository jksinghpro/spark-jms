package jk.bigdata.tech.jms


import javax.jms.Session

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.jms.JmsStreamingSource
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.{DataStreamReader, OutputMode}
import org.apache.spark.sql.types.StructType


/**
  * Created by exa00015 on 17/12/18.
  */
private[jms] class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider with DataSourceRegister with StreamSourceProvider with StreamSinkProvider{

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    parameters.get("queue") match {
      case Some(queue) => new JmsDatasourceRelation(sqlContext, parameters)
      case None => throw new IllegalArgumentException("Option queue is needed")
    }
  }


  override def shortName(): String = "jms"

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    if (mode == SaveMode.Overwrite) {
      throw new UnsupportedOperationException("Data in a kafka topic cannot be overidden !"
        + " Delete topic to implement this functionality")
    }
    data.foreachPartition(rowIter => {
      val connection = DefaultSource.connectionFactory(parameters).createConnection
      val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
      val queue = session.createQueue(parameters.getOrElse("queue",throw new IllegalArgumentException("Option queue is required")))
      val producer = session.createProducer(queue)
      rowIter.foreach(
        record => {
          val msg = session.createTextMessage(record.toString())
          producer.send(msg)
        })
      producer.close
      connection.close
      session.close
    }
    )
    new DataFrameRelation(sqlContext, data)
  }

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String, parameters: Map[String, String]): (String, StructType) = {
    require(schema.isEmpty, "JMS source has a fixed schema and cannot be set with a custom one")
    (shortName,ScalaReflection.schemaFor[JmsMessage].dataType.asInstanceOf[StructType])
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {
    new JmsStreamingSource(sqlContext,parameters, metadataPath, true)
  }

  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    new JmsStreamingSink(sqlContext,parameters)
  }
}


object DefaultSource {

  def connectionFactory(parameters: Map[String, String]) = parameters("connection") match {
    case "amq" => new AMQConnectionFactoryProvider().createConnection(parameters)
    case "rmq" => new RMQConnectionFactoryProvider().createConnection(parameters)
    case "kafka" => new KafkaConnectionFactoryProvider().createConnection(parameters)
    case connection: String => Class.forName(connection).newInstance.asInstanceOf[ConnectionFactoryProvider].createConnection(parameters)
  }

}
