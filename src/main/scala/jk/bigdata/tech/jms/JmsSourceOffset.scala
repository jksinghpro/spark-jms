package jk.bigdata.tech.jms

import org.apache.spark.sql.execution.streaming.Offset

/**
  * Created by exa00015 on 26/12/18.
  */
case class JmsSourceOffset(val id:Long) extends Offset {

  override def json(): String = id.toString

}
