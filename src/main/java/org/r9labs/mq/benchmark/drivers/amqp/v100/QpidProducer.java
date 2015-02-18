/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.amqp.v100;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.qpid.amqp_1_0.client.AcknowledgeMode;
import org.apache.qpid.amqp_1_0.client.Connection;
import org.apache.qpid.amqp_1_0.client.ConnectionException;
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
    
    public QpidProducer (String host, int port, String user, String pass, String target, String subject, int frameSize, String remoteHost, boolean useSSL, int windowSize, AcknowledgeMode mode, String linkName, boolean transactions) throws ConnectionException, Sender.SenderCreationException {
        Container container = new Container (); // new Container(containerName)
        conn = new Connection (host, port, user, pass, frameSize, container, remoteHost, useSSL);
        
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
            txn.commit();
        }
        try {
            pout.close();
        } catch (Sender.SenderClosingException ex) {
            Logger.getLogger(QpidProducer.class.getName()).log(Level.WARNING, "An exception occurred stopping sender", ex);
        }
        conn.close();
    }

    @Override
    public boolean sendMessage(byte[] message) {
        Properties props = new Properties ();
        if (subject != null) {
            props.setSubject(subject);
        }
        Section body = new Data (new Binary (message));
        Section[] sections = {props, body};
        pout.send(new Message (Arrays.asList(sections)), txn);
        
        return true;
    }
    
}
