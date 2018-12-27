package jk.bigdata.tech.jms

import java.util.Properties
import javax.jms.ConnectionFactory

import com.rabbitmq.jms.admin.RMQConnectionFactory
import io.confluent.kafka.jms.KafkaConnectionFactory
import org.apache.activemq.ActiveMQConnectionFactory

/**
  * Created by exa00015 on 24/12/18.
  */
trait ConnectionFactoryProvider {

  def createConnection(options:Map[String,String]):ConnectionFactory

}

class AMQConnectionFactoryProvider extends ConnectionFactoryProvider {

  override def createConnection(options: Map[String, String]): ConnectionFactory = {
    new ActiveMQConnectionFactory
  }
}

class RMQConnectionFactoryProvider extends ConnectionFactoryProvider {

  override def createConnection(options: Map[String, String]): ConnectionFactory = {
    val connectionFactory = new RMQConnectionFactory
    connectionFactory.setUsername(options.getOrElse("username",throw new IllegalArgumentException("Option username is required")))
    connectionFactory.setPassword(options.getOrElse("password",throw new IllegalArgumentException("Option password is required")))
    connectionFactory.setVirtualHost(options.getOrElse("virtualhost",throw new IllegalArgumentException("Option virtualhost is required")))
    connectionFactory.setHost(options.getOrElse("host",throw new IllegalArgumentException("Option host is required")))
    connectionFactory.setPort(options.getOrElse("port",throw new IllegalArgumentException("Option port is required")).toInt)
    connectionFactory
  }
}

class KafkaConnectionFactoryProvider extends ConnectionFactoryProvider {

  override def createConnection(options: Map[String, String]): ConnectionFactory = {
    val props = new Properties
    props.put("bootstrap.servers", options.getOrElse("bootstrap.servers",throw new IllegalArgumentException("Option bootstrap.servers is required")))
    props.put("zookeeper.connect", options.getOrElse("zookeeper.connect",throw new IllegalArgumentException("Option zookeeper.connect is required")))
    props.put("client.id", options.getOrElse("client.id",throw new IllegalArgumentException("Option client.id is required" )))
    val connectionFactory = new KafkaConnectionFactory(props)
    connectionFactory
  }

}

