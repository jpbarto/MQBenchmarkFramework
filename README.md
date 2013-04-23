MQBrokerBenchmarkFramework
==========================

MQBrokerBenchmarkFramework is intended to become a benchmarking framework for messaging oriented middleware systems, inspired by YCSB.  It consists of framework classes for sending / receiving messages and a collection of drivers which can be added to if needed for connecting to brokers over JMS, AMQP, STOMP, MQTT, etc.

More drivers will be made available shortly.  I've completed the AMQP 0.9.1 driver as a test of the framework.  Next to arrive will be AMQP 1.0 using both the SwiftMQ libraries as well as the QPid libraries, followed shortly by a JMS driver which of course *should* be able to accept any JMS-compliant driver.

Once these are completed I'll be moving to write STOMP and MQTT drivers.  Contributed drivers are welcome.
