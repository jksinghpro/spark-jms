package jk.bigdata.tech.jms

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by exa00015 on 21/12/18.
  */
object SparkApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Sql starter")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    val sparkSession = sqlContext.sparkSession

    val result = sparkSession.readStream
      .format("jk.bigdata.tech.jms")
      .option("connection","amq")
      .option("queue","customerQueue")
        .option("username","guest")
        .option("password","guest")
        .option("virtualhost","/")
        .option("host","localhost")
        .option("port","5672")
      .load

    val query = result.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination

    /*
    import sparkSession.implicits._
    val df = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "connect-configs")
      .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val query = df.writeStream
      .outputMode("append")
      .format("console")
      .start()
    //result.show(20,false)
    /*result.write.mode(SaveMode.Append)
      .format("jk.bigdata.tech.jms")
      .option("connection","jk.bigdata.tech.jms.AMQConnectionFactoryProvider")
      .option("queue","customerQueue")
      .save*/
      */
  }

}
