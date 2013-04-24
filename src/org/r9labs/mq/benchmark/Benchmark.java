/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.r9labs.mq.benchmark;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.logging.Formatter;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.r9labs.mq.benchmark.drivers.DriverFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

class GetOne {

    public Integer getOne() {
        return new Integer(1);
    }
}

class BenchmarkThread extends Thread {

    private boolean runFlag = false;
    public long iterCount = 0;
    private GetOne driver;

    public BenchmarkThread() {
        driver = new GetOne();
    }

    public void stopThread() {
        runFlag = false;
    }

    public void run() {
        runFlag = true;

        while (runFlag) {
            Integer value = driver.getOne();
            iterCount += value.longValue();
        }
    }
}

class MessageHandler implements ConsumedMessageHandler, ProducedMessageHandler {

    private long sentSeqNo = 0;
    private long recvSeqNo = 0;

    @Override
    public void handleConsumedMessage(long sentMessageID, long recvMessageID, long sentTS, long recvTS) {
        System.out.println(recvSeqNo + "," + sentMessageID + "," + recvMessageID + "," + sentTS + "," + recvTS);
        recvSeqNo++;
    }

    @Override
    public void handleProducedMessage(long msgID, long sentTimestamp) {
        System.out.println(sentSeqNo + "," + msgID + "," + sentTimestamp);
        sentSeqNo++;
    }
}

final class ReportFormatter extends Formatter {

    @Override
    public String format(LogRecord lr) {
        StringBuilder sb = new StringBuilder();
        sb.append(lr.getMessage() + "\n");
        return sb.toString();
    }
}

public class Benchmark {

    private static final long MicroSecPerNS = 1000;

    public static void main(String[] args) {
        ConsoleHandler ch = new ConsoleHandler();
        ch.setFormatter(new ReportFormatter());
        ch.setLevel(Level.FINE);
        Logger reporter = Logger.getLogger(Benchmark.class.getName());
        reporter.setLevel(Level.FINE);
        reporter.addHandler(ch);
        reporter.setUseParentHandlers(false);

        Options opts = new Options()
                .addOption("duration", true, "Duration of test in seconds")
                .addOption("mcount", true, "Number of messages to send / recv before stopping")
                .addOption("interval", true, "How often to report performance (seconds)")
                .addOption("csv", false, "Report using comma separated values")
                .addOption("pmo", false, "Print a report for every message sent or received")
                .addOption("msize", true, "Size of messages to send (bytes)")
                .addOption("rlimit", true, "Maximum number of messages/s to send")
                .addOption("pc", true, "Number of publishing connections to make")
                .addOption("cc", true, "Number of consuming connections to make")
                .addOption("df", true, "Class name of driver factory")
                .addOption("props", true, "Driver Properties File")
                .addOption("log", true, "Name of logfile")
                .addOption("silent", false, "Print minimal output")
                .addOption("help", false, "Print this message");

        CommandLineParser parser = new GnuParser();
        CommandLine clopts = null;
        try {
            clopts = parser.parse(opts, args);
        } catch (ParseException ex) {
            reporter.log(Level.SEVERE, null, ex);
        }

        int duration = (clopts.hasOption("duration")) ? Integer.valueOf(clopts.getOptionValue("duration")) : Integer.MAX_VALUE;
        int mcount = (clopts.hasOption("mcount")) ? Integer.valueOf(clopts.getOptionValue("mcount")) : 0;
        int rptInterval = (clopts.hasOption("interval")) ? Integer.valueOf(clopts.getOptionValue("interval")) : 1;
        boolean csvOutput = (clopts.hasOption("csv")) ? true : false;
        boolean perMsgOutput = (clopts.hasOption("pmo")) ? true : false;
        int payloadSize = (clopts.hasOption("msize")) ? Integer.valueOf(clopts.getOptionValue("msize")) : 0;
        long sendLimit = (clopts.hasOption("rlimit")) ? Long.valueOf(clopts.getOptionValue("rlimit")) : 0;
        int pCount = (clopts.hasOption("pc")) ? Integer.valueOf(clopts.getOptionValue("pc")) : 1;
        int cCount = (clopts.hasOption("cc")) ? Integer.valueOf(clopts.getOptionValue("cc")) : 1;
        String logFile = (clopts.hasOption("log")) ? clopts.getOptionValue("log") : null;
        String driverFactoryClassName = (clopts.hasOption("df")) ? clopts.getOptionValue("df") : "org.r9labs.mq.benchmark.drivers.blank.BlankFactory";
        String propFilename = (clopts.hasOption("props")) ? clopts.getOptionValue("props") : null;
        boolean silent = clopts.hasOption("silent");

        if ((mcount > 0 && sendLimit <= 0) || (mcount > 0 && sendLimit > mcount)) {
            sendLimit = mcount;
        }

        if (clopts.hasOption("silent")) {
            ch.setLevel(Level.CONFIG);
        }

        DriverFactory driverFactory = null;
        if (driverFactoryClassName != null) {
            try {
                ClassLoader cl = Benchmark.class.getClassLoader();
                Class driverFactoryClass = cl.loadClass(driverFactoryClassName);
                driverFactory = (DriverFactory) driverFactoryClass.newInstance();
            } catch (Exception ex) {
                reporter.log(Level.SEVERE, "Error loading the specified driver factory: " + driverFactoryClassName, ex);
                System.exit(1);
            }
        }

        if (clopts.hasOption("help")) {
            HelpFormatter fmt = new HelpFormatter();
            fmt.printHelp("Benchmark", opts);

            if (clopts.hasOption("df")) {
                System.out.println("===== DRIVER USAGE =====");
                System.out.println(driverFactory.getUsage());
            }
            System.exit(0);
        }

        if (logFile != null) {
            try {
                FileHandler fh = new FileHandler(logFile);
                fh.setFormatter(new ReportFormatter());
                fh.setLevel(Level.FINE);
                Logger.getLogger("").addHandler(fh);
                reporter.addHandler(fh);
            } catch (IOException ex) {
                reporter.log(Level.SEVERE, "Error opening log file " + logFile + " for logging.", ex);
            } catch (SecurityException ex) {
                reporter.log(Level.SEVERE, "Security exception creating handler for logfile " + logFile, ex);
            }
        }

        Properties properties = new Properties();
        if (propFilename != null) {
            try {
                FileReader propFile = new FileReader(propFilename);
                properties.load(propFile);
            } catch (FileNotFoundException ex) {
                reporter.log(Level.SEVERE, "Specified properties file " + propFilename + " not found.", ex);
            } catch (IOException ex) {
                reporter.log(Level.WARNING, "Error loading specified properties file.", ex);
            }
        }
        driverFactory.initialize(properties);

        StringBuilder header = new StringBuilder()
                .append("\nProps File      : ").append(propFilename)
                .append("\nMsg Size (bytes): ").append(payloadSize)
                .append("\nProducers       : ").append(pCount)
                .append("\nConsumers       : ").append(cCount)
                .append("\nRate Limit      : ").append(sendLimit)
                .append("\nMessage Count   : ").append(mcount)
                .append("\nDuration        : ").append(duration)
                .append("\nReport Interval : ").append(rptInterval)
                .append("\nCSV Output      : ").append(csvOutput)
                .append("\nPer Msg Out     : ").append(perMsgOutput)
                .append("\nSilent          : ").append(clopts.hasOption("silent"))
                .append("\nLogging to      : ").append(logFile)
                .append("\nDriver Factory  : ").append(driverFactoryClassName);

        if (propFilename != null) {
            Enumeration<Object> keys = properties.keys();
            header.append("\n===== PROPERTIES =====");
            while (keys.hasMoreElements()) {
                Object key = keys.nextElement();
                header.append("\n").append(key.toString()).append(": ").append(properties.getProperty(key.toString()));
            }
        }
        reporter.log(Level.CONFIG, header.append("\n").toString());

        // First benchmark raw iteration count
        reporter.log(Level.INFO, "Calculating raw average operations / sec...");
        BenchmarkThread bt1 = new BenchmarkThread();
        BenchmarkThread bt2 = new BenchmarkThread();
        bt1.start();
        bt2.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
            reporter.log(Level.WARNING, "Interrupted while waiting 1 second for iteration benchmark");
        }
        bt1.stopThread();
        bt2.stopThread();
        reporter.log(Level.INFO, "Thread 1 / Thread 2: " + (bt1.iterCount / 1) + " / " + (bt2.iterCount / 1) + " average operations per second.");
        bt1 = null;
        bt2 = null;

        ProducerThread[] producers = new ProducerThread[pCount];
        ConsumerThread[] consumers = new ConsumerThread[cCount];
        MessageHandler msgHandler = null;
        if (perMsgOutput) {
            msgHandler = new MessageHandler();
        }

        try {
            for (int i = 0; i < pCount; i++) {
                producers[i] = new ProducerThread(driverFactory, payloadSize, sendLimit);
                if (perMsgOutput) {
                    producers[i].setMessageHandler(msgHandler);
                }
            }
            for (int i = 0; i < cCount; i++) {
                consumers[i] = new ConsumerThread(driverFactory);
                if (perMsgOutput) {
                    consumers[i].setMessageHandler(msgHandler);
                }
            }

            for (ProducerThread pt : producers) {
                pt.start();
            }
            for (ConsumerThread ct : consumers) {
                ct.start();
            }

            long totalMsgSent = 0;
            long totalMsgRecv = 0;
            long totalLatency = 0;

            long startTS = System.currentTimeMillis();
            long currentTS = startTS;
            long lastReportTS = startTS;
            if (csvOutput && !perMsgOutput) {
                reporter.fine("Time Step,sent/s,recv/s,min,avg,max latency(microsec)");
            }

            duration *= 1000;
            rptInterval *= 1000;
            while ((mcount <= 0 && (currentTS - startTS) <= duration)
                    || (mcount > 0 && totalMsgSent < mcount && totalMsgRecv < mcount)) {
                if ((currentTS - lastReportTS) >= rptInterval) {
                    long sentMsgCount = 0;
                    for (ProducerThread p : producers) {
                        sentMsgCount += p.statsMessageCount;
                        p.resetStats();
                    }

                    long recvMsgCount = 0;
                    long minLatency = -1;
                    long avgLatency = -1;
                    long maxLatency = -1;
                    for (ConsumerThread c : consumers) {
                        recvMsgCount += c.statsMessageCount;
                        totalLatency += c.statsTotalLatency;
                        avgLatency += c.statsTotalLatency;
                        if (minLatency == -1 || c.statsMinLatency < minLatency) {
                            minLatency = c.statsMinLatency / MicroSecPerNS;
                        }
                        if (c.statsMaxLatency > maxLatency) {
                            maxLatency = c.statsMaxLatency / MicroSecPerNS;
                        }
                        c.resetStats();
                    }

                    if (recvMsgCount > 0) {
                        avgLatency = (avgLatency / recvMsgCount) / MicroSecPerNS;
                    } else {
                        avgLatency = -1;
                    }

                    totalMsgSent += sentMsgCount;
                    totalMsgRecv += recvMsgCount;
                    int sendRate = (int) (sentMsgCount / (rptInterval / 1000));
                    int recvRate = (int) (recvMsgCount / (rptInterval / 1000));

                    if (!perMsgOutput) {
                        StringBuilder line = new StringBuilder();
                        if (csvOutput) {
                            line.append((currentTS - startTS) / 1000);
                            line.append(",").append(sendRate);
                            line.append(",").append(recvRate);
                            line.append(",").append(minLatency).append(",").append(avgLatency).append(",").append(maxLatency);
                            reporter.fine(line.toString());
                        } else {
                            line.append("Time Step ").append((currentTS - startTS) / 1000).append("s: ");
                            line.append("Sent (msg/s): ").append(sendRate);
                            line.append(", Recv (msg/s): ").append(recvRate);
                            line.append(", Latency (min/avg/max microsec): ").append(minLatency).append("/").append(avgLatency).append("/").append(maxLatency);
                            reporter.fine(line.toString());
                        }
                    }

                    lastReportTS = currentTS;
                } else {
                    Thread.sleep(rptInterval);
                }
                currentTS = System.currentTimeMillis();
            }

            for (ProducerThread p : producers) {
                p.stopThread();
            }
            for (ConsumerThread c : consumers) {
                c.stopThread();
            }

            boolean threadsRunning;
            do {
                threadsRunning = false;
                for (ProducerThread p : producers) {
                    if (p.isAlive()) {
                        threadsRunning = true;
                    }
                }
                for (ConsumerThread c : consumers) {
                    if (c.isAlive()) {
                        threadsRunning = true;
                    }
                }

                if (threadsRunning) {
                    Thread.sleep(100);
                }
            } while (threadsRunning);

            long stopTS = System.currentTimeMillis();
            float actDuration = (stopTS - startTS) / 1000;

            for (int i = 0; i < pCount; i++) {
                totalMsgSent += producers[i].statsMessageCount;
            }
            for (int i = 0; i < cCount; i++) {
                totalMsgRecv += consumers[i].statsMessageCount;
                totalLatency += consumers[i].statsTotalLatency;
            }

            StringBuilder footer = new StringBuilder();
            footer.append("\nFinal Report: ");
            footer.append("\nActual Duration: ").append(actDuration).append("s");
            footer.append("\nSent ").append(totalMsgSent).append(" messages");
            footer.append("\nReceived ").append(totalMsgRecv).append(" messages");
            footer.append("\nAvg Send Rate: ").append(totalMsgSent / actDuration).append(" msg/s");
            footer.append("\nAvg Recv Rate: ").append(totalMsgRecv / actDuration).append(" msg/s");
            if (totalMsgRecv > 0) {
                footer.append("\nAvg Latency: ").append((totalLatency / totalMsgRecv) / MicroSecPerNS).append(" microsec");
            }

            reporter.info(footer.toString());
        } catch (Exception ex) {
            reporter.log(Level.SEVERE, "Error performing benchmark: {0}", ex);
        }

        System.exit(0);
    }
}
