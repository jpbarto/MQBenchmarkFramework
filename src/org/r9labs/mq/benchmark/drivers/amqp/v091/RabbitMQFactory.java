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
    String exchange;
    String extype;
    String routingKey;
    String queueName;
    boolean tcpNoDelay;
    int sendBufferSize;
    int recvBufferSize;
    private int driverCount = 0;

    @Override
    public void initialize(Properties config) {
        host = (config.containsKey("amqp.broker.host")) ? config.getProperty("amqp.broker.host") : "127.0.0.1";
        port = (config.containsKey("amqp.broker.port")) ? Integer.valueOf(config.getProperty("amqp.broker.port")) : 5672;
        username = (config.containsKey("amqp.broker.username")) ? config.getProperty("amqp.broker.username") : "guest";
        password = (config.containsKey("amqp.broker.password")) ? config.getProperty("amqp.broker.password") : "guest";
        exchange = (config.containsKey("amqp.exchange")) ? config.getProperty("amqp.exchange") : "";
        extype = (config.containsKey("amqp.exchange.type")) ? config.getProperty("amqp.exchange.type") : "direct";
        routingKey = (config.containsKey("amqp.routingkey")) ? config.getProperty("amqp.routingkey") : "a.b.c";
        queueName = (config.containsKey("amqp.queue")) ? config.getProperty("amqp.queue") : null;
        tcpNoDelay = (config.containsKey("amqp.socket.tcpNoDelay")) ? Boolean.valueOf(config.getProperty("amqp.socket.tcpNoDelay")) : true;
        sendBufferSize = (config.containsKey("amqp.socket.sendBufferSize")) ? Integer.valueOf(config.getProperty("amqp.socket.sendBufferSize")) : 20481;
        recvBufferSize = (config.containsKey("amqp.socket.recvBufferSize")) ? Integer.valueOf(config.getProperty("amqp.socket.recvBufferSize")) : 20481;
    }

    @Override
    public String getUsage () {
        StringBuffer usage = new StringBuffer ();
        usage.append("RabbitMQFactory Properties File Values:\n");
        usage.append("\n");
        usage.append("amqp.broker.host           : The IP or hostname of the AMQP broker\n");
        usage.append("amqp.broker.port           : Port number on which the broker is listening\n");
        usage.append("amqp.broker.username       : Username to connect with\n");
        usage.append("amqp.broker.password       : Password to authenticate user\n");
        usage.append("amqp.exchange              : Name of the exchange to bind to\n");
        usage.append("amqp.exchange.type         : The named exchanges type [topic, fanout, direct]\n");
        usage.append("amqp.routingkey            : What routing key to publish to or receive from\n");
        usage.append("amqp.queue                 : The name of the queue to bind to; one will be created if not specified\n");
        usage.append("amqp.socket.tcpNoDelay     : {true|false} Enable | Disable TCP No delay on network sockets\n");
        usage.append("amqp.socket.sendBufferSize : The size of the sockets sending buffer");
        usage.append("amqp.socket.recvBufferSize : The size of the sockets receiving buffer");
        return usage.toString();
    }

    @Override
    public ProducingDriver createProducingDriver() {
        driverCount++;
        try {
            return new RabbitMQProducer (host, port, username, password, tcpNoDelay, sendBufferSize, exchange, extype, routingKey);
        } catch (IOException ex) {
            Logger.getLogger(RabbitMQFactory.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public ConsumingDriver createConsumingDriver() {
        driverCount++;

        try {
            return new RabbitMQConsumer(host, port, username, password, tcpNoDelay, recvBufferSize, exchange, extype, routingKey, 1000);
        } catch (IOException ex) {
            Logger.getLogger(RabbitMQFactory.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
}
