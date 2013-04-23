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

    public RabbitMQConsumer(String hostname, int port, String username, String password, final boolean tcpNoDelay, final int recvBuffSize, String exchangeName, String exchangeType, String routingKey, int prefetchSize) throws IOException {
        super(hostname, port, username, password, tcpNoDelay, recvBuffSize);

        chan.basicQos(prefetchSize);

        cin = new QueueingConsumer(chan);

        chan.exchangeDeclare(exchangeName, exchangeType);

        String queueName = chan.queueDeclare().getQueue();
        chan.queueBind(queueName, exchangeName, routingKey);
        chan.basicConsume(queueName, autoAck, cin);
    }

    public RabbitMQConsumer(String hostname, int port, String username, String password, final boolean tcpNoDelay, final int recvBuffSize, String exchangeName, String exchangeType, String routingKey, int prefetchSize, int ackBatchSize) throws IOException {
        super(hostname, port, username, password, tcpNoDelay, recvBuffSize);

        this.ackBatchSize = ackBatchSize;
        this.autoAck = false;

        chan.basicQos(prefetchSize);

        cin = new QueueingConsumer(chan);

        chan.exchangeDeclare(exchangeName, exchangeType);

        String queueName = chan.queueDeclare().getQueue();
        chan.queueBind(queueName, exchangeName, routingKey);
        chan.basicConsume(queueName, autoAck, cin);
    }

    public RabbitMQConsumer(String hostname, int port, String username, String password, final boolean tcpNoDelay, final int recvBuffSize, String queueName, int prefetchSize) throws IOException {
        super(hostname, port, username, password, tcpNoDelay, recvBuffSize);

        chan.basicQos(prefetchSize);

        cin = new QueueingConsumer(chan);

        chan.basicConsume(queueName, autoAck, cin);
    }

    public RabbitMQConsumer(String hostname, int port, String username, String password, final boolean tcpNoDelay, final int recvBuffSize, String queueName, int prefetchSize, int ackBatchSize) throws IOException {
        super(hostname, port, username, password, tcpNoDelay, recvBuffSize);

        this.ackBatchSize = ackBatchSize;
        this.autoAck = false;

        chan.basicQos(prefetchSize);

        cin = new QueueingConsumer(chan);

        chan.basicConsume(queueName, autoAck, cin);
    }

    public RabbitMQConsumer(String hostname, int port, String username, String password, final boolean tcpNoDelay, final int recvBuffSize, int prefetchSize) throws IOException {
        super(hostname, port, username, password, tcpNoDelay, recvBuffSize);

        chan.basicQos(prefetchSize);

        cin = new QueueingConsumer(chan);
        String queueName = chan.queueDeclare().getQueue();
        chan.basicConsume(queueName, autoAck, cin);
    }

    public RabbitMQConsumer(String hostname, int port, String username, String password, final boolean tcpNoDelay, final int recvBuffSize, int prefetchSize, int ackBatchSize) throws IOException {
        super(hostname, port, username, password, tcpNoDelay, recvBuffSize);

        this.ackBatchSize = ackBatchSize;
        this.autoAck = false;

        chan.basicQos(prefetchSize);

        cin = new QueueingConsumer(chan);
        String queueName = chan.queueDeclare().getQueue();
        chan.basicConsume(queueName, autoAck, cin);
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
