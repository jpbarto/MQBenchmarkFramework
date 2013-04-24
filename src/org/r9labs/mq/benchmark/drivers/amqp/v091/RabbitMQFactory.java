/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.amqp.v091;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
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

    String amqpURI;
    int frameMax;
    int heartbeat;
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
        amqpURI = (config.containsKey("amqp.broker.uri")) ? config.getProperty("amqp.broker.uri") : "amqp://127.0.0.1";
        frameMax = (config.containsKey("amqp.broker.frameMax")) ? Integer.valueOf(config.getProperty("amqp.broker.frameMax")) : -1;
        heartbeat = (config.containsKey("amqp.broker.heartbeat")) ? Integer.valueOf(config.getProperty("amqp.broker.heartbeat")) : -1;
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
        cackBatch = (config.containsKey("amqp.consumer.ackEvery")) ? Integer.valueOf(config.getProperty("amqp.consumer.ackEvery")) : -1;
    }

    @Override
    public String getUsage() {
        StringBuffer usage = new StringBuffer();
        usage.append("RabbitMQFactory Properties File Values:\n");
        usage.append("\n");
        usage.append("amqp.broker.uri             : The URI for the AMQP broker\n");
        usage.append("amqp.broker.frameMax        : Maximum frame size\n");
        usage.append("amqp.broker.heartbeat       : Send a heartbeat signal every heartbeat seconds\n");
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
        usage.append("amqp.consumer.ackEvery      : Batch acknowledgement by only sending an acknowledge message ackEvery messages\n");
        return usage.toString();
    }

    @Override
    public ProducingDriver createProducingDriver() {
        driverCount++;
        try {
            if (pqueueName != null) {
                return new RabbitMQProducer(amqpURI, frameMax, heartbeat, tcpNoDelay, sendBufferSize, pqueueName);
            } else {
                return new RabbitMQProducer(amqpURI, frameMax, heartbeat, tcpNoDelay, sendBufferSize, pexchange, pextype, proutingKey);
            }
        } catch (Exception ex) {
            Logger.getLogger(RabbitMQFactory.class.getName()).log(Level.SEVERE, "Error creating producing driver", ex);
        } 
        return null;
    }

    @Override
    public ConsumingDriver createConsumingDriver() {
        driverCount++;

        try {
            RabbitMQConsumer ret;
            
            if (cqueueName != null) {
                ret = new RabbitMQConsumer (amqpURI, frameMax, heartbeat, tcpNoDelay, recvBufferSize, cqueueName, cprefetch);
            }else{
                ret = new RabbitMQConsumer (amqpURI, frameMax, heartbeat, tcpNoDelay, recvBufferSize, cexchange, cextype, croutingKey, cprefetch);
            }

            if (cackBatch > 0) {
                ret.setAckBatchSize(cackBatch);
            }

            return ret;
        } catch (Exception ex) {
            Logger.getLogger(RabbitMQFactory.class.getName()).log(Level.SEVERE, null, ex);
        } 
        return null;
    }
}
