/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.amqp.v091;

import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.r9labs.mq.benchmark.drivers.ConsumingDriver;

/**
 *
 * @author jpbarto
 */
public class RabbitMQConsumer extends RabbitMQDriverBase implements ConsumingDriver {

    QueueingConsumer cin;
    boolean autoAck = true;
    private int acksWaiting = 0;
    private int ackBatchSize = 1;
    private String queueName = null;
    private String exchangeName = null;
    private String exchangeType = null;
    private String routingKey = null;

    public RabbitMQConsumer(String amqpURI, final boolean tcpNoDelay, final int recvBuffSize, String exchangeName, String exchangeType, String routingKey, int prefetchSize) throws IOException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        super(amqpURI, tcpNoDelay, recvBuffSize);

         chan.basicQos(prefetchSize);
         this.exchangeName = exchangeName;
         this.exchangeType = exchangeType;
         this.routingKey = routingKey;
    }

    public RabbitMQConsumer(String amqpURI, final boolean tcpNoDelay, final int recvBuffSize, String queueName, int prefetchSize) throws IOException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        super(amqpURI, tcpNoDelay, recvBuffSize);

        chan.basicQos(prefetchSize);
        this.queueName = queueName;
    }

    public RabbitMQConsumer(String amqpURI, final boolean tcpNoDelay, final int recvBuffSize, int prefetchSize) throws IOException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        super(amqpURI, tcpNoDelay, recvBuffSize);

        chan.basicQos(prefetchSize);
    }
    
    public void setAckBatchSize (int ackBatchSize) {
        this.ackBatchSize = ackBatchSize;
    }

    @Override
    public void start() {
        if (ackBatchSize > 0) {
            autoAck = false;
        }

        cin = new QueueingConsumer(chan);

        if (queueName == null) {
            try {
                queueName = chan.queueDeclare().getQueue();
            } catch (IOException ex) {
                Logger.getLogger(RabbitMQConsumer.class.getName()).log(Level.SEVERE, "Error trying to declare a queue: " + queueName, ex);
            }
        }

        if (exchangeName != null) {
            try {
                chan.exchangeDeclare(exchangeName, exchangeType);
                chan.queueBind(queueName, exchangeName, routingKey);
            } catch (IOException ex) {
                Logger.getLogger(RabbitMQConsumer.class.getName()).log(Level.SEVERE, "Error binding to exchange: "+ exchangeName, ex);
            }
        }

        try {
            chan.basicConsume(queueName, autoAck, cin);
        } catch (IOException ex) {
            Logger.getLogger(RabbitMQConsumer.class.getName()).log(Level.SEVERE, "Error while preparing to consume from queue: " + queueName, ex);
        }
    }

    @Override
    public void stop() {
        try {
            chan.close();
            conn.close();
        } catch (IOException ex) {
            Logger.getLogger(RabbitMQConsumer.class.getName()).log(Level.SEVERE, "Error closing consumer connection.", ex);
        }
    }

    @Override
    public byte[] getMessage() {
        QueueingConsumer.Delivery delivery = null;

        try {
            delivery = cin.nextDelivery(500);
        } catch (InterruptedException ex) {
            Logger.getLogger(RabbitMQConsumer.class.getName()).log(Level.SEVERE, "Consumer interrupted while waiting for a message.", ex);
        } catch (ShutdownSignalException ex) {
            Logger.getLogger(RabbitMQConsumer.class.getName()).log(Level.SEVERE, "Consumer recieved shutdown signal while waiting for a message.", ex);
        } catch (ConsumerCancelledException ex) {
            Logger.getLogger(RabbitMQConsumer.class.getName()).log(Level.SEVERE, "Consumer cancelled while waiting for a message.", ex);
        }

        if (delivery == null) {
            return null;
        }
        acksWaiting++;

        Envelope envelope = delivery.getEnvelope();
        byte[] msg = delivery.getBody();
        if (!autoAck && acksWaiting >= ackBatchSize) {
            try {
                chan.basicAck(envelope.getDeliveryTag(), true);
            } catch (IOException ex) {
                Logger.getLogger(RabbitMQConsumer.class.getName()).log(Level.SEVERE, null, ex);
                return null;
            }
            acksWaiting = 0;
        }

        return msg;
    }
}
