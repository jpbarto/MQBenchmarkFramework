/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers;

import java.util.HashMap;
import java.util.Properties;

/**
 *
 * @author jpbarto
 */
public interface DriverFactory {
    public void initialize (Properties p);
    public String getUsage ();
    public ProducingDriver createProducingDriver ();
    public ConsumingDriver createConsumingDriver ();
}
