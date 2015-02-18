/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark.drivers.blank;

import java.util.HashMap;
import java.util.Properties;
import org.r9labs.mq.benchmark.drivers.ConsumingDriver;
import org.r9labs.mq.benchmark.drivers.ProducingDriver;
import org.r9labs.mq.benchmark.drivers.DriverFactory;

/**
 *
 * @author jpbarto
 */
public class BlankFactory implements DriverFactory {

    @Override
    public void initialize(Properties p) {
    }

    @Override
    public ProducingDriver createProducingDriver() {
        return new BlankDriver ();
    }
    
    @Override
    public ConsumingDriver createConsumingDriver () {
        return new BlankDriver ();
    }

    @Override
    public String getUsage() {
        return "Blank is a simple driver that pretends to send / recv messages; no configuration needed.";
    }
}
