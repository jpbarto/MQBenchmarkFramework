/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.amqp.v091;

import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.r9labs.mq.benchmark.drivers.ConsumingDriver;
import org.r9labs.mq.benchmark.drivers.ProducingDriver;
import org.r9labs.mq.benchmark.drivers.DriverFactory;

/**
 *
 * @author jpbarto
 */
public class RabbitMQFactory implements DriverFactory {

    String host;
    int port;
    String username;
    String password;
    boolean tcpNoDelay;
    int sendBufferSize;
    int recvBufferSize;
    String pexchange;
    String pextype;
    String proutingKey;
    String pqueueName;
    String cexchange;
    String cextype;
    String croutingKey;
    String cqueueName;
    int cprefetch;
    int cackBatch;
    private int driverCount = 0;

    @Override
    public void initialize(Properties config) {
        host = (config.containsKey("amqp.broker.host")) ? config.getProperty("amqp.broker.host") : "127.0.0.1";
        port = (config.containsKey("amqp.broker.port")) ? Integer.valueOf(config.getProperty("amqp.broker.port")) : 5672;
        username = (config.containsKey("amqp.broker.username")) ? config.getProperty("amqp.broker.username") : "guest";
        password = (config.containsKey("amqp.broker.password")) ? config.getProperty("amqp.broker.password") : "guest";

        tcpNoDelay = (config.containsKey("amqp.socket.tcpNoDelay")) ? Boolean.valueOf(config.getProperty("amqp.socket.tcpNoDelay")) : true;
        sendBufferSize = (config.containsKey("amqp.socket.sendBufferSize")) ? Integer.valueOf(config.getProperty("amqp.socket.sendBufferSize")) : 20481;
        recvBufferSize = (config.containsKey("amqp.socket.recvBufferSize")) ? Integer.valueOf(config.getProperty("amqp.socket.recvBufferSize")) : 20481;

        pexchange = (config.containsKey("amqp.producer.exchange")) ? config.getProperty("amqp.producer.exchange") : "amq.direct";
        pextype = (config.containsKey("amqp.producer.exchange.type")) ? config.getProperty("amqp.producer.exchange.type") : "direct";
        proutingKey = (config.containsKey("amqp.producer.routingkey")) ? config.getProperty("amqp.producer.routingkey") : "a.b.c";
        pqueueName = (config.containsKey("amqp.producer.queue")) ? config.getProperty("amqp.producer.queue") : null;

        cexchange = (config.containsKey("amqp.consumer.exchange")) ? config.getProperty("amqp.consumer.exchange") : "amq.direct";
        cextype = (config.containsKey("amqp.consumer.exchange.type")) ? config.getProperty("amqp.consumer.exchange.type") : "direct";
        croutingKey = (config.containsKey("amqp.consumer.routingkey")) ? config.getProperty("amqp.consumer.routingkey") : "a.b.c";
        cqueueName = (config.containsKey("amqp.consumer.queue")) ? config.getProperty("amqp.consumer.queue") : null;
        cprefetch = (config.containsKey("amqp.consumer.prefetch")) ? Integer.valueOf(config.getProperty("amqp.consumer.prefetch")) : 1000;
        cackBatch = (config.containsKey("amqp.consumer.ackBatch")) ? Integer.valueOf(config.getProperty("amqp.consumer.ackBatch")) : -1;
    }

    @Override
    public String getUsage() {
        StringBuffer usage = new StringBuffer();
        usage.append("RabbitMQFactory Properties File Values:\n");
        usage.append("\n");
        usage.append("amqp.broker.host            : The IP or hostname of the AMQP broker\n");
        usage.append("amqp.broker.port            : Port number on which the broker is listening\n");
        usage.append("amqp.broker.username        : Username to connect with\n");
        usage.append("amqp.broker.password        : Password to authenticate user\n");
        usage.append("amqp.socket.tcpNoDelay      : {true|false} Enable | Disable TCP No delay on network sockets\n");
        usage.append("amqp.socket.sendBufferSize  : The size of the sockets sending buffer");
        usage.append("amqp.socket.recvBufferSize  : The size of the sockets receiving buffer");
        usage.append("amqp.producer.exchange      : Name of the exchange to send messages to\n");
        usage.append("amqp.producer.exchange.type : The named producer exchanges type [topic, fanout, direct]\n");
        usage.append("amqp.producer.routingkey    : Routing key for publisher\n");
        usage.append("amqp.producer.queue         : The name of the queue to send messages to; one will be created if not specified\n");
        usage.append("amqp.consumer.exchange      : Name of the exchange to consumer messages from\n");
        usage.append("amqp.consumer.exchange.type : The named consumer exchanges type [topic, fanout, direct]\n");
        usage.append("amqp.consumer.routingkey    : Routing key for consumer\n");
        usage.append("amqp.consumer.queue         : The name of the queue to consume from; one will be created if not specified\n");
        usage.append("amqp.consumer.prefetch      : The number of messages to prefetch\n");
        usage.append("amqp.consumer.ackBatch      : Batch acknowledge messages by waiting to recieve ackBatch messages before sending an acknowledge message.\n");
        return usage.toString();
    }

    @Override
    public ProducingDriver createProducingDriver() {
        driverCount++;
        try {
            if (pqueueName != null) {
                return new RabbitMQProducer(host, port, username, password, tcpNoDelay, sendBufferSize, pqueueName);
            } else {
                return new RabbitMQProducer(host, port, username, password, tcpNoDelay, sendBufferSize, pexchange, pextype, proutingKey);
            }
        } catch (IOException ex) {
            Logger.getLogger(RabbitMQFactory.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public ConsumingDriver createConsumingDriver() {
        driverCount++;

        try {
            if (cqueueName != null) {
                if (cackBatch > 0) {
                    return new RabbitMQConsumer(host, port, username, password, tcpNoDelay, recvBufferSize, cqueueName, cprefetch, cackBatch);
                } else {
                    return new RabbitMQConsumer(host, port, username, password, tcpNoDelay, recvBufferSize, cqueueName, cprefetch);
                }
            }

            if (cackBatch > 0) {
                return new RabbitMQConsumer(host, port, username, password, tcpNoDelay, recvBufferSize, cexchange, cextype, croutingKey, cprefetch, cackBatch);
            }

            return new RabbitMQConsumer(host, port, username, password, tcpNoDelay, recvBufferSize, cexchange, cextype, croutingKey, cprefetch);
        } catch (IOException ex) {
            Logger.getLogger(RabbitMQFactory.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
}
