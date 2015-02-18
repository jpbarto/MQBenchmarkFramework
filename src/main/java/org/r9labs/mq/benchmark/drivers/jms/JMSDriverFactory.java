/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.jms;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.r9labs.mq.benchmark.drivers.ConsumingDriver;
import org.r9labs.mq.benchmark.drivers.DriverFactory;
import org.r9labs.mq.benchmark.drivers.ProducingDriver;

/**
 *
 * @author jpbarto
 */
public class JMSDriverFactory implements DriverFactory {
    private Context context = null;
    private String brokerUsername;
    private String brokerPassword;
    
    private String ptopic;
    private String pqueue;
    
    private String ctopic;
    private String cqueue;
    
    @Override
    public void initialize(Properties properties) {
        brokerUsername = (properties.containsKey("jmsfactory.broker.username")) ? properties.getProperty("jmsfactory.broker.username") : null;
        brokerPassword = (properties.containsKey("jmsfactory.broker.password")) ? properties.getProperty("jmsfactory.broker.password") : null;
        
        ptopic = (properties.containsKey("jmsfactory.producer.topic")) ? properties.getProperty("jmsfactory.producer.topic") : null;
        pqueue = (properties.containsKey("jmsfactory.producer.queue")) ? properties.getProperty("jmsfactory.producer.queue") : null;
        
        ctopic = (properties.containsKey("jmsfactory.consumer.topic")) ? properties.getProperty("jmsfactory.consumer.topic") : null;
        cqueue = (properties.containsKey("jmsfactory.consumer.queue")) ? properties.getProperty("jmsfactory.consumer.queue") : null;
        try {
            context = new InitialContext (properties);
        } catch (NamingException ex) {
            Logger.getLogger(JMSDriverFactory.class.getName()).log(Level.SEVERE, "Error creating InitialContext from properties", ex);
        }      
    }

    @Override
    public String getUsage() {
        StringBuilder sb = new StringBuilder ()
                .append("jmsfactory.broker.username : Username to be used when connecting to the JMS broker\n")
                .append("jmsfactory.broker.password : Password to be used when connecting to the JMS broker\n")
                .append("jmsfactory.producer.topic  : Topic to which messages should be sent\n")
                .append("jmsfactory.producer.queue  : Queue to which messages should be sent\n")
                .append("jmsfactory.consumer.topic  : Topic from which messages should be retrieved\n")
                .append("jmsfactory.consumer.queue  : Queue from which messages should be retrieved\n");
        return sb.toString();
    }

    @Override
    public ProducingDriver createProducingDriver() {
        JMSProducer ret = new JMSProducer (context);
        if (brokerUsername != null) {
            ret.setLogin (brokerUsername, brokerPassword);
        }
        
        if (pqueue != null) {
            ret.setQueue (pqueue);
        }
        
        if (ptopic != null) {
            ret.setTopic (ptopic);
        }
        return ret;
    }

    @Override
    public ConsumingDriver createConsumingDriver() {        
        JMSConsumer ret = new JMSConsumer (context);
        if (brokerUsername != null) {
            ret.setLogin (brokerUsername, brokerPassword);
        }
        
        if (cqueue != null) {
            ret.setQueue (cqueue);
        }
        
        if (ctopic != null) {
            ret.setTopic (ctopic);
        }
        return ret;
    }
    
}
