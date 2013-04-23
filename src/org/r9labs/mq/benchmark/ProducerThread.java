/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Random;
import org.r9labs.mq.benchmark.drivers.ProducingDriver;
import org.r9labs.mq.benchmark.drivers.DriverFactory;

/**
 *
 * @author jpbarto
 */
public class ProducerThread extends Thread {

    boolean runFlag = true;
    protected long sendLimit = 0;
    protected int payloadSize = 0;
    protected byte[] dataPayload;
    protected ProducingDriver driver;
    protected ProducedMessageHandler msgHandler = null;
    public long statsMessageCount = 0;

    public ProducerThread(DriverFactory df, int payloadSize, long sendLimit) {
        this.driver = df.createProducingDriver();
        this.payloadSize = payloadSize;
        this.sendLimit = sendLimit;

        dataPayload = new byte[16 + payloadSize];
        if (payloadSize > 0) {
            Random rnd = new Random ();
            rnd.setSeed (System.nanoTime());
            rnd.nextBytes(dataPayload);
        }
    }

    public void setMessageHandler(ProducedMessageHandler h) {
        msgHandler = h;
    }

    public void stopThread() {
        runFlag = false;
    }
    
    public void resetStats () {
        statsMessageCount = 0;
    }

    @Override
    public void run() {
        driver.start ();
        
        long seqNo = 0;
        ByteBuffer messageBytes = ByteBuffer.wrap (dataPayload);     

        while (runFlag) {
            if (sendLimit == 0 || statsMessageCount < sendLimit) {
                long sentTS = System.currentTimeMillis();
                messageBytes.putLong(0, seqNo);
                messageBytes.putLong(8, sentTS);

                // sb.append (delim).append(sentTS).append(delim).append(seqNo);
                if (driver.sendMessage(messageBytes.array())) {
                    if (msgHandler != null) {
                        msgHandler.handleProducedMessage(seqNo, sentTS);
                    }

                    seqNo++;
                    statsMessageCount++;
                }
                //sb.delete(sbLen, sb.length());
            }
        }
        
        driver.stop();
    }
}
