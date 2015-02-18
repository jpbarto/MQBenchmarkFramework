/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.amqp.v091;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.r9labs.mq.benchmark.drivers.ProducingDriver;

/**
 *
 * @author jpbarto
 */
public class RabbitMQProducer implements ProducingDriver {
    private Connection conn = null;
    private Channel chan = null;
    private String exchangeName = null;
    private String routingKey = null;
    private String queueName = null;
    private int txSize = 0;
    private long totalMsgSent = 0;

    public RabbitMQProducer(ConnectionFactory connectionF, String exchangeName, String exchangeType, String routingKey) throws IOException {
        conn = connectionF.newConnection();
        chan = conn.createChannel();
        
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;

        chan.exchangeDeclare(exchangeName, exchangeType);
    }

    public RabbitMQProducer(ConnectionFactory connectionF, String queueName) throws IOException {
        conn = connectionF.newConnection();
        chan = conn.createChannel();
        
        this.queueName = queueName;
    }
    
    public void setTxSize (int txSize) {
        this.txSize = txSize;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        try {
            chan.close();
            conn.close();
        } catch (IOException ex) {
            Logger.getLogger(RabbitMQProducer.class.getName()).log(Level.SEVERE, "Error stopping producing driver", ex);
        }
    }

    @Override
    public boolean sendMessage(byte[] message) {
        try {
            if (queueName != null) {
                chan.basicPublish("", queueName, MessageProperties.MINIMAL_BASIC, message);
            } else {
                chan.basicPublish(exchangeName, routingKey, MessageProperties.MINIMAL_BASIC, message);
            }
            totalMsgSent++;
            
            if (txSize > 0 && totalMsgSent % txSize == 0) {
                chan.txCommit();
            }
        } catch (IOException ex) {
            Logger.getLogger(RabbitMQProducer.class.getName()).log(Level.SEVERE, "Error publishing message", ex);
            return false;
        }
        return true;
    }
}
