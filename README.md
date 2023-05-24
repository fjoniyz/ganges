# GANGES - Gewährleistung von Anonymitäts-Garantien in Enterprise-Streaminganwendungen

A project for the module (Advanced) Distributed Systems Prototyping at the Technical University of Berlin. 


## Run via docker

To start the docker compose simply run
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

