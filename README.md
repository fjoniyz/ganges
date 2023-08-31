# GANGES - Gewährleistung von Anonymitäts-Garantien in Enterprise-Streaminganwendungen

A project for the module (Advanced) Distributed Systems Prototyping at the Technical University of Berlin. 

The interceptor interface is implemented from two different viewpoints. 

1. Producer_Interceptor:
The data is changed through an interceptor before the data reaches the Kafka Cluster.

2. Consumer_Interceptor: 
The data is the same; However, the consumer has a different output than the data of the CGCluster.

## Run via docker

The Kafka Broker and Redis cache can be started via docker compose. simply run
```
docker compose up
```

To manually display a specific topic's content in the console, one can use
```
docker exec --interactive --tty broker \
      kafka-console-consumer --bootstrap-server broker:9092 \
                             --topic <topic-name> \
                             --from-beginning
```

To start the Pipe, MetricsCollector or BenchmarkConsumer, use

```
mvn clean install
mvn exec:java -Dexec.mainClass=<full-class-name>
```
where `<full-class-name>` is class name with package name: \
For Pipe use `com.ganges.anoniks.Pipe`, \
For remote metrics collector use `com.ganges.benchmark.RemoteMetricsCollector` \
For benchmark consumer use `com.ganges.benchmark.BenchmarkConsumer`.