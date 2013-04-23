/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.amqp.v091;

import com.rabbitmq.client.MessageProperties;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.r9labs.mq.benchmark.drivers.ProducingDriver;

/**
 *
 * @author jpbarto
 */
public class RabbitMQProducer extends RabbitMQDriverBase implements ProducingDriver {

    private String exchangeName = null;
    private String routingKey = null;
    private String queueName = null;

    public RabbitMQProducer(String hostname, int port, String username, String password, final boolean tcpNoDelay, final int sendBuffSize, String exchangeName, String exchangeType, String routingKey) throws IOException {
        super(hostname, port, username, password, tcpNoDelay, sendBuffSize);
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;

        chan.exchangeDeclare(exchangeName, exchangeType);
    }

    public RabbitMQProducer(String hostname, int port, String username, String password, final boolean tcpNoDelay, final int sendBuffSize, String queueName) throws IOException {
        super(hostname, port, username, password, tcpNoDelay, sendBuffSize);
        this.queueName = queueName;
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
                chan.basicPublish("", queueName, MessageProperties.BASIC, message);
            } else {
                chan.basicPublish(exchangeName, routingKey, MessageProperties.BASIC, message);
            }
        } catch (IOException ex) {
            Logger.getLogger(RabbitMQProducer.class.getName()).log(Level.SEVERE, "Error publishing message", ex);
            return false;
        }
        return true;
    }
}
