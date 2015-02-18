/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.amqp.v091;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
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
public class RabbitMQConsumer implements ConsumingDriver {

    private Connection conn = null;
    private Channel chan = null;
    private QueueingConsumer cin;
    private boolean autoAck = true;
    private int acksWaiting = 0;
    private int ackBatchSize = 1;
    private int txSize = 0;
    private long totalMsgCount = 0;
    private String queueName = null;
    private String exchangeName = null;
    private String exchangeType = null;
    private String routingKey = null;

    public RabbitMQConsumer(ConnectionFactory connectionF) throws IOException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        conn = connectionF.newConnection();
        chan = conn.createChannel();
    }

    public void setExchange(String name, String type, String routingKey) {
        this.exchangeName = name;
        this.exchangeType = type;
        this.routingKey = routingKey;
    }

    public void setQueue(String queueName) {
        this.queueName = queueName;
    }

    public void setAckBatchSize(int ackBatchSize) {
        this.ackBatchSize = ackBatchSize;
        if (ackBatchSize > 0) {
            autoAck = false;
        } else {
            autoAck = true;
        }
    }

    public void setPrefetch(int prefetchSize) throws IOException {
        chan.basicQos(prefetchSize);
    }

    public void setTxSize(int txSize) {
        this.txSize = txSize;
    }

    @Override
    public void start() {
        cin = new QueueingConsumer(chan);

        if (queueName != null) {
            try {
                conn.createChannel().queueDeclarePassive(queueName);
            } catch (IOException ex) {
                try {
                    // The queue does not exist or is owned exclusively by someone else; OR an error occurred, try to redeclare presuming the queue does not exist
                    chan.queueDeclare(queueName, false, false, true, null);
                } catch (IOException ex1) {
                    Logger.getLogger(RabbitMQConsumer.class.getName()).log(Level.SEVERE, "Error caught by consumer trying to declare queue: " + queueName, ex);
                }
            }
        } else {
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
                Logger.getLogger(RabbitMQConsumer.class.getName()).log(Level.SEVERE, "Error binding to exchange: " + exchangeName, ex);
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
        totalMsgCount++;

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

        if (txSize != 0 && totalMsgCount % txSize == 0) {
            try {
                chan.txCommit();
            } catch (IOException ex) {
                Logger.getLogger(RabbitMQConsumer.class.getName()).log(Level.SEVERE, "IO Exception trying to perform txCommit", ex);
            }
        }

        return msg;
    }
}
