/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark;

/**
 *
 * @author jpbarto
 */
public interface ProducedMessageHandler {
    public void handleProducedMessage (long messageID, long sentTS);
}
