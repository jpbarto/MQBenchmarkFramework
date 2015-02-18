/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.jms;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.NamingException;
import org.r9labs.mq.benchmark.drivers.ConsumingDriver;

/**
 *
 * @author jpbarto
 */
public class JMSConsumer implements ConsumingDriver {

    private String username = null;
    private String password = null;
    private String topicName = null;
    private Topic topic = null;
    private String queueName = null;
    private Queue queue = null;
    private Context context = null;
    private Session session = null;
    private Connection conn = null;
    private MessageConsumer cin = null;

    public JMSConsumer(Context context) {
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
       try {
            ConnectionFactory connF;
            try {
                connF = (ConnectionFactory) context.lookup("ConnectionFactory");
            } catch (NamingException ex) {
                Logger.getLogger(JMSProducer.class.getName()).log(Level.SEVERE, "Error retrieving connection factory from context", ex);
                return;
            }

            if (username != null) {
                conn = connF.createConnection(username, password);
            } else {
                conn = connF.createConnection();
            }
            conn.start();

            session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException ex) {
            Logger.getLogger(JMSProducer.class.getName()).log(Level.SEVERE, "Error connecting to broker and creating a session", ex);
        }

        if (topicName != null) {
            try {
                topic = session.createTopic(topicName);
                cin = session.createConsumer(topic);
            } catch (JMSException ex) {
                Logger.getLogger(JMSProducer.class.getName()).log(Level.SEVERE, "Error creating message producer for topic: " + topicName, ex);
            }
        }else if (queueName != null) {
            try {
                queue = session.createQueue(queueName);
                cin = session.createConsumer(queue);
            } catch (JMSException ex) {
                Logger.getLogger(JMSProducer.class.getName()).log(Level.SEVERE, "Error creating message producer for queue: "+ queueName, ex);
            }
        }else{
            try {
                queue = session.createTemporaryQueue();
                cin = session.createConsumer (queue);
            } catch (JMSException ex) {
                Logger.getLogger(JMSProducer.class.getName()).log(Level.SEVERE, "Error creating temporary queue (no queue or topic provided)", ex);
            }
        }
    }

    @Override
    public void stop() {
        try {
            cin.close();
            session.close();
            conn.stop();
            conn.close();
        } catch (JMSException ex) {
            Logger.getLogger(JMSProducer.class.getName()).log(Level.WARNING, "An error occurred stopping consumer connection to broker", ex);
        }
    }

    @Override
    public byte[] getMessage() {
        BytesMessage msg;
        try {
            msg = (BytesMessage) cin.receive(500);
        } catch (JMSException ex) {
            Logger.getLogger(JMSConsumer.class.getName()).log(Level.SEVERE, "Error receiving BytesMessage", ex);
            return null;
        }
        
        if (msg != null) {
            try {
                byte[] ret = new byte[(int)msg.getBodyLength()];
                msg.readBytes(ret);
                msg.acknowledge();
                return ret;
            } catch (JMSException ex) {
                Logger.getLogger(JMSConsumer.class.getName()).log(Level.SEVERE, "Error reading received BytesMessage", ex);
            }
        }
        
        return null;
    }
}
