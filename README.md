# JMS Compatible DataSource for Apache Spark


**spark-jms** is a spark dataframe reader and writer for JMS(Java Messaging Service) compatible sources .This connector works in both streaming and batch mode.
It works directly or indirectly with mostly also the sources which has JMS providers implemented for them.


# Usage

Currently spark-jms is not available in maven central or any other public repository.This is marked as a TODO for me.

*spark-jms* is used normally like other connectors just like kakfa.

For using it Clone this repo on your system.Ensure maven is installed on your system for building it.

    git clone https://github.com/jksinghpro/spark-jms.git
    
Go to root directory of the project and run 

    mvn clean install

Once the project is build succesfully,either add it as a dependency to your spark application and add its jars to the spark classpath.
You can also pass the jar to the spark submit using option

    spark-submit --jars <jar_path>

# Extensiblity 

This connector directly supports ActiveMq, RabbitMq, Kafka (Will go on adding more direct support soon).
However indirectly it supports all the messaging queues or sources which have JMS clients.
Only requirement is that those JMS connection factory implementation for those sources must be in spark classpath along with implementation to the following trait.

    jk.bigdata.tech.jms.ConnectionFactoryProvider

This trait has an abstract method 
    
    def createConnection(options:Map[String,String]):ConnectionFactory
    
which returns an dynamic instance of ConnectionFactory for the source under consideration.
For examples on implementing this trait . Refer [here](https://github.com/jksinghpro/spark-jms/blob/master/src/main/scala/jk/bigdata/tech/jms/ConnectionFactoryProvider.scala)

## Sample Usage

Sample code for usage of spark-jms connector

### Batch

```scala

//Active MQ
val dataframeAmq = spark.read
      .format("jk.bigdata.tech.jms")
      .option("connection","amq")
      .load
      
//Rabbit MQ      
val dataframeRmq = spark.read
      .format("jk.bigdata.tech.jms")
      .option("connection","rmq")
      .option("queue","<<queue_name>>")
      .option("username","<<username>>")
      .option("password","<<password>>")
      .option("virtualhost","<<virtual_host>>")
      .option("host","<<host>>")
      .option("port","<<port>>")
      .load
      

 //Kafka
 val dataframeKafka = spark.read
       .format("jk.bigdata.tech.jms")
       .option("connection","kafka")
       .option("bootstrap.servers","<<bootstrap-servers>>")
       .option("zookeeper.connect","<<zookeeper-host>>")
       .option("client.id","<<client id>>")
       .option("queue","<<queue_name>>")
       .load
       
 dataframe.write
       .mode(SaveMode.Append)
       .format("jk.bigdata.tech.jms")
       .option("connection","jk.bigdata.tech.jms.AMQConnectionFactoryProvider")
       .option("queue","<<queue name>>")
       .save
       
 dataframe.write
        .mode(SaveMode.Append)
        .format("jk.bigdata.tech.jms")
        .option("connection","rmq")
        .option("queue","<<queue name>>")
        .save
 
 dataframe.write
        .mode(SaveMode.Append)
        .format("jk.bigdata.tech.jms")
        .option("connection","kafka")
        .option("queue","<<queue name>>")
        .save

```
### Streaming

```scala

//Active MQ
val dataframeAmq = spark.readStream
      .format("jk.bigdata.tech.jms")
      .option("connection","amq")
      .load
      
//Rabbit MQ      
val dataframeRmq = spark.readStream
      .format("jk.bigdata.tech.jms")
      .option("connection","rmq")
      .option("queue","<<queue_name>>")
      .option("username","<<username>>")
      .option("password","<<password>>")
      .option("virtualhost","<<virtual_host>>")
      .option("host","<<host>>")
      .option("port","<<port>>")
      .load
      

 //Kafka
 val dataframeKafka = spark.readStream
       .format("jk.bigdata.tech.jms")
       .option("connection","kafka")
       .option("bootstrap.servers","<<bootstrap-servers>>")
       .option("zookeeper.connect","<<zookeeper-host>>")
       .option("client.id","<<client id>>")
       .option("queue","<<queue_name>>")
       .load
       
       

```




## Configuration Paramters(Common) :



| Option | Required | Description | DefaultValue |
| :---: | :---: | :---: | :---: | 
| Queue | true | Name of topic or messaging queue | None |
| acknowledge | false | To acknowledge the message or not | false |
| connection | true | Fully Qualified Name of ConnectionFactoryProviderImplementation or Alias of directly supported queues  | None |


**Note** Above configurations are common across jms clients .Other than these configs some configs are specific to JMS client under consideration
 

# Issues

- Issue Tracker: https://github.com/jksinghpro/spark-jms/issues


# License

The project is licensed under the Apache 2 license.

