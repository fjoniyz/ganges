# Documentation

## Data and Use Cases
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

Schema: 
```
{'GERAET_NR':string, 'GERAET_ZAEHLERPLATZ':string, 'GERAET_ZAEHLERTYP':string, 'GERAET_WFAKTOR':int, 'MESSART':string}
```

#### E-Mobilität
- Transformed with Kafka and loaded in the database
- The data is not raised in different phases of the Costumer Journey -> data assigned to specific use case
- `timeseries_id` is a hashed int value
```
{'Timestamp':string, 'timeseries_id':string, 'value':string}
```

## Anonymization Algorithms

### &delta;-Doca [Link](https://link.springer.com/chapter/10.1007/978-3-030-00305-0_20)

Implementation [Link](https://github.com/itsjorgemg/TFM-deltaDoca)

#### Summary:


### CASTLEGUARD [Link](https://ieeexplore.ieee.org/abstract/document/9251212)

Implementation [Link](https://github.com/hallnath1/CASTLEGUARD/tree/master)

#### Summary:

CASTLEGUARD is a data stream anonymization approach that provides a reliable guarantee of k-anonymity, l-diversity, and non-interactive differential privacy based on parameters l, β, and φ. It achieves differential privacy for data streams by sampling entries from an input data stream with probability β and using additive noise taken from a Laplace distribution with mean \$μ=0$ and scale \$b= R/φ$ where \$R$ is the range of an attribute.


| Parameter     | Description   | Default       | 
| ------------- | ------------- | ------------- | 
| l  | used to enforce l-diversity, which ensures that each group of k-anonymized tuples contains at least l different values for the sensitive attribute | |
| k  | used to enforce k-anonymity, which ensures that each quasi-identifier appears at least k-times in a cluster | |
| β  | used for β-sampling, which means that each incoming tuple is randomly sampled/discarded with probability β  | |
| φ  | used for perturbation, which adds noise to the quasi-identifiers in the data stream to protect privacy. A higher value of φ results in more noise being added |
| b  | maximum number of clusters that can be active | |
| δ  | maximum number of tuples that can be active (delay constraint) | |
| μ  |   |




