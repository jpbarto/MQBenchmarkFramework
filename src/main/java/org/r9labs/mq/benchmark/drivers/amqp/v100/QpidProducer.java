/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.amqp.v100;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.qpid.amqp_1_0.client.AcknowledgeMode;
import org.apache.qpid.amqp_1_0.client.Connection;
import org.apache.qpid.amqp_1_0.client.ConnectionErrorException;
import org.apache.qpid.amqp_1_0.client.ConnectionException;
import org.apache.qpid.amqp_1_0.client.LinkDetachedException;
import org.apache.qpid.amqp_1_0.client.Message;
import org.apache.qpid.amqp_1_0.client.Sender;
import org.apache.qpid.amqp_1_0.client.Session;
import org.apache.qpid.amqp_1_0.client.Transaction;
import org.apache.qpid.amqp_1_0.transport.Container;
import org.apache.qpid.amqp_1_0.type.Binary;
import org.apache.qpid.amqp_1_0.type.Section;
import org.apache.qpid.amqp_1_0.type.messaging.Data;
import org.apache.qpid.amqp_1_0.type.messaging.Properties;
import org.r9labs.mq.benchmark.drivers.ProducingDriver;

/**
 *
 * @author jpbarto
 */
public class QpidProducer implements ProducingDriver {
    Connection conn;
    Sender pout;
    Transaction txn = null;
    String subject;
    
    public QpidProducer (String host, int port, String user, String pass, String target, String subject, int frameSize, String remoteHost, boolean useSSL, int windowSize, AcknowledgeMode mode, String linkName, boolean transactions, int maxChannel) throws ConnectionException, Sender.SenderCreationException, LinkDetachedException {
        Container container = new Container (); // new Container(containerName)
        conn = new Connection (host, port, user, pass, frameSize, container, remoteHost, useSSL, maxChannel);
        
        this.subject = subject;
        
        Session session = conn.createSession ();
        pout = session.createSender (target, windowSize, mode, linkName);
        
        if (transactions) {
            txn = session.createSessionLocalTransaction();
        }
    }
    @Override
    public void start() {
    }

    @Override
    public void stop() {
        if (txn != null) {
            try {
                txn.commit();
            } catch (LinkDetachedException ex) {
                Logger.getLogger(QpidProducer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        try {
            pout.close();
        } catch (Sender.SenderClosingException ex) {
            Logger.getLogger(QpidProducer.class.getName()).log(Level.WARNING, "An exception occurred stopping sender", ex);
        }
        try {
            conn.close();
        } catch (ConnectionErrorException ex) {
            Logger.getLogger(QpidProducer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public boolean sendMessage(byte[] message) {
        Properties props = new Properties ();
        if (subject != null) {
            props.setSubject(subject);
        }
        Section body = new Data (new Binary (message));
        Section[] sections = {props, body};
        try {
            pout.send(new Message (Arrays.asList(sections)), txn);
        } catch (LinkDetachedException ex) {
            Logger.getLogger(QpidProducer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (TimeoutException ex) {
            Logger.getLogger(QpidProducer.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return true;
    }
    
}
