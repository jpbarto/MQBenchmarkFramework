/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.blank;

import java.nio.ByteBuffer;
import org.r9labs.mq.benchmark.drivers.ConsumingDriver;
import org.r9labs.mq.benchmark.drivers.ProducingDriver;

/**
 *
 * @author jpbarto
 */
public class BlankDriver implements ProducingDriver, ConsumingDriver {
    ByteBuffer messageBytes = ByteBuffer.wrap ("SuperLongBlankEmptyMessage".getBytes());
    private long seqNo = 0;
    
    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public final byte[] getMessage() {
        messageBytes.putLong(0, seqNo++);
        messageBytes.putLong(8, System.currentTimeMillis());
        return messageBytes.array();
    }

    @Override
    public final boolean sendMessage(byte[] message) {
        return true;
    }
    
}
