/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark;

import java.nio.ByteBuffer;
import org.r9labs.mq.benchmark.drivers.ConsumingDriver;
import org.r9labs.mq.benchmark.drivers.DriverFactory;

/**
 *
 * @author jpbarto
 */
public class ConsumerThread extends Thread {

    boolean runFlag = true;
    protected ConsumingDriver driver;
    protected ConsumedMessageHandler msgHandler = null;
    public long statsMessageCount = 0;
    public long statsMinLatency = -1;
    public long statsMaxLatency = 0;
    public long statsTotalLatency = 0;

    public ConsumerThread(DriverFactory df) {
        driver = df.createConsumingDriver();
    }

    public void stopThread() {
        runFlag = false;
    }

    public void setMessageHandler(ConsumedMessageHandler h) {
        msgHandler = h;
    }

    public void resetStats() {
        statsTotalLatency = 0;
        statsMinLatency = -1;
        statsMaxLatency = 0;
        statsMessageCount = 0;
    }

    @Override
    public void run() {
        driver.start();

        byte[] msgBytes;
        ByteBuffer messageBuf;
        long seqNo = 0;
        long msgSeqNo;
        long sentTS = 0;
        long recvTS;
        long latency;
        final String delim = ":";    
        String attribs[];

        while (runFlag) {
            msgSeqNo = 0;
            msgBytes = driver.getMessage();
            if (msgBytes == null) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                }
                continue;
            }

            messageBuf = ByteBuffer.wrap (msgBytes);
            recvTS = System.currentTimeMillis();
            seqNo++;
            statsMessageCount++;

            if (msgBytes.length >= 16) {
                msgSeqNo = messageBuf.getLong(0);
                sentTS = messageBuf.getLong(8);

                latency = recvTS - sentTS;
                statsTotalLatency += latency;
                if (statsMinLatency < 0 || latency < statsMinLatency) {
                    statsMinLatency = latency;
                }
                if (latency > statsMaxLatency) {
                    statsMaxLatency = latency;
                }
            }

            if (msgHandler != null) {
                msgHandler.handleConsumedMessage(msgSeqNo, seqNo, sentTS, recvTS);
            }
        }

        driver.stop();
    }
}
