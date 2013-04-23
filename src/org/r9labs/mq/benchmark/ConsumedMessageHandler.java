/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark;

/**
 *
 * @author jpbarto
 */
public interface ConsumedMessageHandler {
    public void handleConsumedMessage (long sentMessageID, long receivedMessageID, long sentTS, long receivedTS);
}
