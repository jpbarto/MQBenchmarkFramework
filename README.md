MQBrokerBenchmarkFramework
==========================

MQBrokerBenchmarkFramework, inspired by Yahoo Cloud Service Benchmark (YCSB), is intended to become a benchmarking framework for messaging oriented middleware systems.  It consists of framework classes for sending / receiving messages and a collection of drivers which can be added to if needed for connecting to brokers over JMS, AMQP, STOMP, MQTT, etc.

More drivers will be made available shortly.  I've completed the AMQP 0.9.1 driver as a test of the framework.  Next to arrive will be AMQP 1.0 using both the SwiftMQ libraries as well as the QPid libraries, followed shortly by a JMS driver which of course *should* be able to accept any JMS-compliant driver.

The MQ Broker Benchmark Framework is born out of a recent evaluation of various AMQP and JMS brokers.  Many if not all of the brokers provided their own sample applications and tools for testing the performance of the broker.  This project, like others before it, will try to provide a common framework for evaluationg MOM performance.  Where I think this project differs from other excellent works (many of which can be found on GitHub) is the combination of the testing application with the 'driver' plugins.

Many messaging brokers are now beginning to support multiple protocols, ActiveMQ and RabbitMQ are two examples.  Supporting JMS, AMQP, STOMP, and others, messaging brokers provide multiple avenues for communication.  The MQBBF provides a common method for evaluating each avenue while ensuring commonality and consistency so that a broker's STOMP performance can be compared 1-to-1 with the broker's MQTT performance.

Also with protocols like AMQP 1.0, where multiple client implementations exist (Qpid 1.0 client, Proton, SwiftMQ), the MQBBF can provide a basis for evaluating not only broker performance but client performance as well.
