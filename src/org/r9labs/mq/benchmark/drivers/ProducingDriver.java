/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers;

/**
 *
 * @author jpbarto
 */
public interface ProducingDriver {
    public void start ();
    public void stop ();
    public boolean sendMessage (byte[] message);
}
