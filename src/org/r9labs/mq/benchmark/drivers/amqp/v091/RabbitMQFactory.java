/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.amqp.v091;

import com.rabbitmq.client.ConnectionFactory;
import java.net.Socket;
import java.net.SocketException;
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
    int pTxSize;
    long pConfirm;
    
    String cexchange;
    String cextype;
    String croutingKey;
    String cqueueName;
    int cTxSize;
    int cprefetch;
    int cackBatch;
    boolean cAutoAck;
    
    ConnectionFactory connF;

    @Override
    public void initialize(Properties config) {
        amqpURI = (config.containsKey("amqp.broker.uri")) ? config.getProperty("amqp.broker.uri") : "amqp://127.0.0.1";
        frameMax = (config.containsKey("amqp.broker.frameMax")) ? Integer.valueOf(config.getProperty("amqp.broker.frameMax")) : 0;
        heartbeat = (config.containsKey("amqp.broker.heartbeat")) ? Integer.valueOf(config.getProperty("amqp.broker.heartbeat")) : 0;
        tcpNoDelay = (config.containsKey("amqp.socket.tcpNoDelay")) ? Boolean.valueOf(config.getProperty("amqp.socket.tcpNoDelay")) : true;
        sendBufferSize = (config.containsKey("amqp.socket.sendBufferSize")) ? Integer.valueOf(config.getProperty("amqp.socket.sendBufferSize")) : 0;
        recvBufferSize = (config.containsKey("amqp.socket.recvBufferSize")) ? Integer.valueOf(config.getProperty("amqp.socket.recvBufferSize")) : 0;

        pextype = (config.containsKey("amqp.producer.exchange.type")) ? config.getProperty("amqp.producer.exchange.type") : "direct";
        pexchange = (config.containsKey("amqp.producer.exchange")) ? config.getProperty("amqp.producer.exchange") : pextype;
        proutingKey = (config.containsKey("amqp.producer.routingkey")) ? config.getProperty("amqp.producer.routingkey") : "a.b.c";
        pqueueName = (config.containsKey("amqp.producer.queue")) ? config.getProperty("amqp.producer.queue") : null;
        pTxSize = (config.containsKey("amqp.producer.txsize")) ? Integer.valueOf(config.getProperty("amqp.producer.txsize")) : 0;
 
        cextype = (config.containsKey("amqp.consumer.exchange.type")) ? config.getProperty("amqp.consumer.exchange.type") : "direct";
        cexchange = (config.containsKey("amqp.consumer.exchange")) ? config.getProperty("amqp.consumer.exchange") : cextype;
        croutingKey = (config.containsKey("amqp.consumer.routingkey")) ? config.getProperty("amqp.consumer.routingkey") : "a.b.c";
        cqueueName = (config.containsKey("amqp.consumer.queue")) ? config.getProperty("amqp.consumer.queue") : null;
        cprefetch = (config.containsKey("amqp.consumer.prefetch")) ? Integer.valueOf(config.getProperty("amqp.consumer.prefetch")) : 0;
        cAutoAck = config.containsKey("amqp.consumer.autoack") ? Boolean.valueOf(config.getProperty("amqp.consumer.autoack")) : true;
        cackBatch = (config.containsKey("amqp.consumer.ackEvery")) ? Integer.valueOf(config.getProperty("amqp.consumer.ackEvery")) : -1;
        cTxSize = (config.containsKey("amqp.consumer.txsize")) ? Integer.valueOf(config.getProperty("amqp.consumer.txsize")) : 0;

        if (cackBatch > 0) {
            cAutoAck = false;
        }
        
        connF = new ConnectionFactory() {
            @Override
            public void configureSocket(Socket socket) throws SocketException {
                socket.setTcpNoDelay(tcpNoDelay);
                if (sendBufferSize > 0) {
                    socket.setSendBufferSize(sendBufferSize);
                }
                if (recvBufferSize > 0) {
                    socket.setReceiveBufferSize(recvBufferSize);
                }
            }
        };
        
        try {
            connF.setUri(amqpURI);
        } catch (Exception ex) {
            Logger.getLogger(RabbitMQFactory.class.getName()).log(Level.SEVERE, "Error parsing AMQP URI", ex);
        }
        
        connF.setRequestedFrameMax(frameMax);
        connF.setRequestedHeartbeat(heartbeat);
    }

    @Override
    public String getUsage() {  
        StringBuilder usage = new StringBuilder();
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
        usage.append("amqp.producer.txsize        : How many messages to send before commiting a transaction\n");
        
        usage.append("amqp.consumer.exchange      : Name of the exchange to consumer messages from\n");
        usage.append("amqp.consumer.exchange.type : The named consumer exchanges type [topic, fanout, direct]\n");
        usage.append("amqp.consumer.routingkey    : Routing key for consumer\n");
        usage.append("amqp.consumer.queue         : The name of the queue to consume from; one will be created if not specified\n");
        usage.append("amqp.consumer.prefetch      : The number of messages to prefetch\n");
        usage.append("amqp.consumer.autoack       : Whether to automatically acknowledge every message as its received\n");
        usage.append("amqp.consumer.ackEvery      : Batch acknowledgement by only sending an acknowledge message ackEvery messages\n");
        usage.append("amqp.consumer.txsize        : How many messages to consumer before acknowledging a transaction\n");
        return usage.toString();
    }

    @Override
    public ProducingDriver createProducingDriver() {
    
        try {
            RabbitMQProducer p;
            if (pqueueName != null) {
                p = new RabbitMQProducer(connF, pqueueName);
            } else {
                p = new RabbitMQProducer(connF, pexchange, pextype, proutingKey);
            }
            p.setTxSize(pTxSize);
            
            return p;
        } catch (Exception ex) {
            Logger.getLogger(RabbitMQFactory.class.getName()).log(Level.SEVERE, "Error creating producing driver", ex);
        }
        return null;
    }

    @Override
    public ConsumingDriver createConsumingDriver() {
        try {
            RabbitMQConsumer ret = new RabbitMQConsumer (connF);
            
            if (cexchange != null) {
                ret.setExchange(cexchange, cextype, croutingKey);
            }
            
            if (cqueueName != null) {
                ret.setQueue(cqueueName);
            }
            
            if (cprefetch > 0) {
                ret.setPrefetch(cprefetch);
            }

            if (cackBatch > 0) {
                ret.setAckBatchSize(cackBatch);
            }
            
            if (cTxSize > 0) {
                ret.setTxSize(cTxSize);
            }

            return ret;
        } catch (Exception ex) {
            Logger.getLogger(RabbitMQFactory.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
}
