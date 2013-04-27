/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.jms;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnectionFactory;
import javax.jms.Queue;
import javax.jms.QueueConnectionFactory;
import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import org.r9labs.mq.benchmark.drivers.ProducingDriver;

/**
 *
 * @author jpbarto
 */
public class JMSProducer implements ProducingDriver {

    private String username = null;
    private String password = null;
    private String topicName = null;
    private Topic topic = null;
    private String queueName = null;
    private Queue queue = null;
    private Context context = null;
    private Session session = null;
    private Connection conn = null;
    private MessageProducer pout = null;

    public JMSProducer(Context context) {
        this.context = context;
    }

    public void setLogin(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public void setTopic(String topic) {
        topicName = topic;
    }

    public void setQueue(String queue) {
        queueName = queue;
    }

    @Override
    public void start() {
        if (topicName != null) {
            TopicConnectionFactory connF;
            try {
                connF = (TopicConnectionFactory) context.lookup("ConnectionFactory");
            } catch (NamingException ex) {
                Logger.getLogger(JMSProducer.class.getName()).log(Level.SEVERE, "Error retrieving connection factory from context", ex);
                return;
            }

            try {
                if (username != null) {
                    conn = connF.createTopicConnection(username, password);
                } else {
                    conn = connF.createTopicConnection();
                }
                conn.start();
                session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                topic = session.createTopic(topicName);
                pout = session.createProducer(topic);
            } catch (JMSException ex) {
                Logger.getLogger(JMSProducer.class.getName()).log(Level.SEVERE, "Error creating message producer", ex);
            }
        }
    }

    @Override
    public void stop() {
        try {
            pout.close();
            session.close();
            conn.stop();
            conn.close();
        } catch (JMSException ex) {
            Logger.getLogger(JMSProducer.class.getName()).log(Level.WARNING, "An error occurred stopping producer connection to broker", ex);
        }
    }

    @Override
    public boolean sendMessage(byte[] message) {
        try {
            BytesMessage msg = session.createBytesMessage();
            msg.writeBytes(message);
            pout.send(msg);
            return true;
        } catch (JMSException ex) {
            Logger.getLogger(JMSProducer.class.getName()).log(Level.SEVERE, "Error sending message", ex);
        }
        return false;
    }
}
