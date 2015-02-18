/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.amqp.v100;

import java.util.Collections;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.apache.qpid.amqp_1_0.client.AcknowledgeMode;
import org.apache.qpid.amqp_1_0.type.DistributionMode;
import org.apache.qpid.amqp_1_0.client.Connection;
import org.apache.qpid.amqp_1_0.client.ConnectionErrorException;
import org.apache.qpid.amqp_1_0.client.ConnectionException;
import org.apache.qpid.amqp_1_0.client.LinkDetachedException;
import org.apache.qpid.amqp_1_0.client.Message;
import org.apache.qpid.amqp_1_0.client.Receiver;
import org.apache.qpid.amqp_1_0.client.Session;
import org.apache.qpid.amqp_1_0.client.Transaction;
import org.apache.qpid.amqp_1_0.transport.ConnectionEndpoint;
import org.apache.qpid.amqp_1_0.transport.Container;
import org.apache.qpid.amqp_1_0.type.Section;
import org.apache.qpid.amqp_1_0.type.Symbol;
import org.apache.qpid.amqp_1_0.type.UnsignedInteger;
import org.apache.qpid.amqp_1_0.type.messaging.Data;
import org.apache.qpid.amqp_1_0.type.messaging.ExactSubjectFilter;
import org.apache.qpid.amqp_1_0.type.messaging.Filter;
import org.apache.qpid.amqp_1_0.type.messaging.MatchingSubjectFilter;
import org.r9labs.mq.benchmark.drivers.ConsumingDriver;

/**
 *
 * @author jpbarto
 */
public class QpidConsumer implements ConsumingDriver {
    private Connection conn;
    private Session session;
    private Receiver cin;
    private Transaction txn = null;
    private int txnSize = 0;
    private int txnCount = 0;
    private int windowSize;
    private int credit = 0;
    
    public QpidConsumer (String host, int port, String user, String pass, String source, String filterStr, int frameSize, String remoteHost, boolean useSSL, int windowSize, AcknowledgeMode amode, DistributionMode dmode, String linkName, boolean isDurable, int txnSize, int maxChannel) throws ConnectionException, LinkDetachedException {
        Container container = new Container (); // new Container(containerName)
        conn = new Connection (host, port, user, pass, frameSize, container, remoteHost, useSSL, maxChannel);
               
        this.windowSize = windowSize;
        
        Filter filter = null;
        if (filterStr != null) {
            String[] filterParts = filterStr.split ("=", 2);
            if ("exact-subject".equals(filterParts[0])) {
                filter = new ExactSubjectFilter (filterParts[1]);
            }else if ("matching-subject".equals(filterParts[0])) {
                filter = new MatchingSubjectFilter(filterParts[1]);
            }else{
                Logger.getLogger(QpidConsumer.class.getName()).warn("Unknown filter type: "+ filterParts[0]);
            }
        }
        session = conn.createSession ();
        cin = session.createReceiver(source, dmode, amode, linkName, isDurable, Collections.singletonMap(Symbol.valueOf("filter"), filter), null);
        
        if (txnSize > 0) {
            this.txnSize = txnSize;
            txn = session.createSessionLocalTransaction();
        }
    }
    
    @Override
    public void start() {
    }

    @Override
    public void stop() {
        cin.close();
        session.close();
        try {
            conn.close();
        } catch (ConnectionErrorException ex) {
            Logger.getLogger(QpidConsumer.class.getName ()).warn("Error closing connection: "+ ex);
        }
    }

    @Override
    public byte[] getMessage() {
        if (credit == 0) {
            credit = windowSize;
            cin.setCredit(UnsignedInteger.valueOf (credit), false);
        }
        
        Message msg = cin.receive(500);
        credit--;
        
        if (msg == null) {
            return null;
        }
        
        cin.acknowledge(msg.getDeliveryTag(), txn);
        
        if (txn != null) {
            txnCount++;
            if (txnCount >= txnSize) {
                txnCount = 0;
                try {
                    txn.commit();
                } catch (LinkDetachedException ex) {
                    java.util.logging.Logger.getLogger(QpidConsumer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        
        return ((Data)msg.getPayload ().get(1)).getValue().getArray();
    }
    
}
