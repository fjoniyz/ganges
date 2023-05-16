### Data Schemas (from Ampeers Energy)

- Kafka not used for Data on Buidings, Generators, Consumers, storage ... (no Streaming Data)
- Streaming Data is collected within different Phases of Costumer Journey
- as datatype String collected and processed in Kafka Stream (gecastet, rounded, plausibilized, etc.) and assigned to an ID.

Within in Database:
- Timestamp, timeseries_id, value (Float /Integer / String)

#### Mieterstrom 
- mieterstromrelevanten Daten initially per csv-lists 
- Datatransfer per csv-Wechsellisten or MSCONS

- API to make this data available for OPERATE tech

#### E-Mobilität
- Transformed with Kafka and loaded in the database
- The data is not raised in different phases of the Costumer Journey -> data assigned to specific use case
