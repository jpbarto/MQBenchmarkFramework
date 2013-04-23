/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.amqp.v091;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

/**
 *
 * @author jpbarto
 */
public class RabbitMQDriverBase {
    protected Connection conn = null;
    protected Channel chan = null;

    public RabbitMQDriverBase (String hostname, int port, String username, String password, final boolean tcpNoDelay, final int bufferSize) throws IOException {
        ConnectionFactory connF = new ConnectionFactory() {
            @Override
            public void configureSocket(Socket socket) throws SocketException {
                socket.setTcpNoDelay(tcpNoDelay);
                socket.setReceiveBufferSize(bufferSize);
                socket.setSendBufferSize(bufferSize);
            }
        };
        connF.setHost(hostname);
        connF.setPort(port);
        connF.setUsername(username);
        connF.setPassword(password);

        conn = connF.newConnection();
        chan = conn.createChannel();
    }
}
