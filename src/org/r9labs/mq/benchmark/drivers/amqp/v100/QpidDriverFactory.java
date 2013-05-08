/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.amqp.v100;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.qpid.amqp_1_0.client.AcknowledgeMode;
import org.apache.qpid.amqp_1_0.client.ConnectionException;
import org.apache.qpid.amqp_1_0.type.DistributionMode;
import org.apache.qpid.amqp_1_0.type.messaging.StdDistMode;
import org.r9labs.mq.benchmark.drivers.ConsumingDriver;
import org.r9labs.mq.benchmark.drivers.DriverFactory;
import org.r9labs.mq.benchmark.drivers.ProducingDriver;

/**
 *
 * @author jpbarto
 */
public class QpidDriverFactory implements DriverFactory {
    // Broker Parameters
    private String hostname;
    private int port;
    private String username;
    private String password;
    
    // Producer Parameters
    private String target;
    private String pSubject;
    private int pFrameSize;
    private String pRemoteHost;
    private boolean pUseSSL;
    private int pWindowSize;
    private AcknowledgeMode pAckMode;
    private String pLinkName;
    private boolean pUseTrans;
    
    // Consumer Parameters
    private String source;
    private String cFilter;
    private int cFrameSize;
    private String cRemoteHost;
    private boolean cUseSSL;
    private int cWinSize;
    private AcknowledgeMode cAckMode;
    private DistributionMode cDistMode;
    private String cLinkName;
    private boolean cDurable;
    private boolean cUseTrans;
    private int cTxnSize;

    @Override
    public void initialize(Properties properties) {
        hostname = (properties.containsKey("qpiddriver.broker.hostname")) ? properties.getProperty("qpiddriver.broker.hostname") : "localhost";
        port = (properties.containsKey("qpiddriver.broker.port")) ? Integer.valueOf(properties.getProperty("qpiddriver.broker.port")) : 5672;
        username = (properties.containsKey("qpiddriver.broker.username")) ? properties.getProperty("qpiddriver.broker.username") : null;
        password = (properties.containsKey("qpiddriver.broker.password")) ? properties.getProperty("qpiddriver.broker.password") : null;

        target = (properties.containsKey("qpiddriver.producer.target")) ? properties.getProperty("qpiddriver.producer.target") : null;
        pSubject = (properties.containsKey("qpiddriver.producer.subject")) ? properties.getProperty("qpiddriver.producer.subject") : null;
        pFrameSize = (properties.containsKey("qpiddriver.producer.frameSize")) ? Integer.valueOf(properties.getProperty("qpiddriver.producer.frameSize")) : 65536;
        pRemoteHost = (properties.containsKey("qpiddriver.producer.remoteHost")) ? properties.getProperty("qpiddriver.producer.remoteHost") : hostname;
        pUseSSL = (properties.containsKey("qpiddriver.producer.useSSL")) ? Boolean.valueOf(properties.getProperty("qpiddriver.producer.useSSL")) : false;
        pWindowSize = (properties.containsKey("qpiddriver.producer.windowSize")) ? Integer.valueOf(properties.getProperty("qpiddriver.producer.windowSize")) : 100;
        pAckMode = (properties.containsKey("qpiddriver.producer.ackMode")) ? AcknowledgeMode.valueOf(properties.getProperty("qpiddriver.producer.ackMode")) : AcknowledgeMode.ALO;
        pLinkName = (properties.containsKey("qpiddriver.producer.linkName")) ? properties.getProperty("qpiddriver.producer.linkName") : null;
        pUseTrans = (properties.containsKey("qpiddriver.producer.useTransactions")) ? Boolean.valueOf(properties.getProperty("qpiddriver.producer.useTransactions")) : false;

        source = (properties.containsKey("qpiddriver.consumer.source")) ? properties.getProperty("qpiddriver.consumer.source") : null;
        cFilter = (properties.containsKey("qpiddriver.consumer.filter")) ? properties.getProperty("qpiddriver.consumer.filter") : null;
        cFrameSize = (properties.containsKey("qpiddriver.consumer.frameSize")) ? Integer.valueOf(properties.getProperty("qpiddriver.consumer.frameSize")) : 65536;
        cRemoteHost = (properties.containsKey("qpiddriver.consumer.remoteHost")) ? properties.getProperty("qpiddriver.consumer.remoteHost") : hostname;
        cUseSSL = (properties.containsKey("qpiddriver.consumer.useSSL")) ? Boolean.valueOf(properties.getProperty("qpiddriver.consumer.useSSL")) : false;
        cWinSize = (properties.containsKey("qpiddriver.consumer.windowSize")) ? Integer.valueOf(properties.getProperty("qpiddriver.consumer.windowSize")) : 100;
        cAckMode = (properties.containsKey("qpiddriver.consumer.ackMode")) ? AcknowledgeMode.valueOf(properties.getProperty("qpiddriver.consumer.ackMode")) : AcknowledgeMode.ALO;
        cDistMode = (properties.containsKey("qpiddriver.consumer.distMode")) ? StdDistMode.valueOf(properties.getProperty("qpiddriver.consumer.distMode")) : StdDistMode.MOVE;
        cLinkName = (properties.containsKey("qpiddriver.consumer.linkName")) ? properties.getProperty("qpiddriver.consumer.linkName") : null;
        cDurable = (properties.containsKey("qpiddriver.consumer.durable")) ? Boolean.valueOf(properties.getProperty("qpiddriver.consumer.durable")) : false;
        cTxnSize = (properties.containsKey("qpiddriver.consumer.transactionSize")) ? Integer.valueOf(properties.getProperty("qpiddriver.consumer.transactionSize")) : 0;
    }

    @Override
    public String getUsage() {
        return "";
    }

    @Override
    public ProducingDriver createProducingDriver() {
        try {
            return new QpidProducer(hostname, port, username, password, target, pSubject, pFrameSize, pRemoteHost, pUseSSL, pWindowSize, pAckMode, pLinkName, pUseTrans);
        } catch (Exception ex) {
            Logger.getLogger(QpidDriverFactory.class.getName()).log(Level.SEVERE, "Error instantiating QpidProducer", ex);
        }
        return null;
    }

    @Override
    public ConsumingDriver createConsumingDriver() {
        try {
            return new QpidConsumer(hostname, port, username, password, source, cFilter, cFrameSize, cRemoteHost, cUseSSL, cWinSize, cAckMode, cDistMode, cLinkName, cDurable, cTxnSize);
        } catch (ConnectionException ex) {
            Logger.getLogger(QpidDriverFactory.class.getName()).log(Level.SEVERE, "Error instantiating QpidConsumer", ex);
        }
        return null;
    }
}
