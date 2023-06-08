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
(based on this [Master Thesis](https://upcommons.upc.edu/handle/2117/386663))

#### Summary:

δ-DOCA is a strategy to anonymize data streams in a non-interactive context, with the addition of noise directly on the data. It consists of two stages: Domain Bounding by δ and DOCA. In the first stage, the data domain is defined and adjusted by a δ, obtaining the sensitivity value of the differential privacy. Then, in the second stage, the stage of utility improvement, an online microaggregation is performed prior to adding noise to data.

| Parameter     | Description   | Default       | 
| ------------- | ------------- | ------------- | 
| ε  | privacy budget | |
| delay constraint  | maximum number of tuples that can be active | |
| b  | maximum number of clusters that can be active | |
| μ  | maximum number of clusters that are used to calculate information loss  | |


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

#### Description:

- for tupel \$t$ in Stream \$S$:
  - if \$random(0,1) > beta$ -> break (= supress \$t$) 
  - else:
     - Perturb tupel \$t$
     - Cluster tupel \$t$: (in other words: look for cluster with most fitting generalization; same technique as delta-Doca Algorithm)
          - Get set of cluster(s) \$C_{min}$ that has the minimum enlargement
          - For each cluster \$c \in C_{min}$
             - check if \$infoLoss(c, t)$ would be smaller than the average information loss \$\tau$
             - if so -> add \$t$ to \$C_{best}$
          - If \$C_{best}$ is empty:
             - if _non-ks-cluster in Memory_ < _allowed non-ks-cluster in memory_ -> Create new cluster and add tuple \$t$
             - Else -> add \$t$ to a cluster \$c_{min} \in C_{min}$ with the smallest size
          - Else -> add \$t$ to a cluster \$c_{best} \in C_{best}$      
     - Once a tupel \$t^{'}$ has reached time-step delta, cluster \$c_{t^{'}}$ are evaluated for publication:
          - if \$c_{t^{'}}.size >= k$:
             - output cluster* \$c$
          - Else: check if tupel \$t^{'}$ is in an already _ks-anonymized_ cluster \$c_{ks-a}$ _(reuse strategy)_
             - if so -> use generalization of \$c_{ks-a}$  
          - Else check if cluster \$c_{t^{'}}$ is an outlier (= smaller than half if the _non-ks-anonymized cluster_)
             - if so -> use the most used generalization
          - Else check if merge with other Clusters is impossible (_Sum of size of each non-ks-cluster_ \$< k$)
             - if so -> use the most used generalization
          - Else 
             - Merge Clusters
                - check enlargement of cluster \$c_{t^{'}}$  after possible merge with _non-ks-cluster_
                - choose cluster with minimum enlargement
                - repeat until \$cluster_{merged} >= k$
              - *Output merged Cluster:
                - Check if Cluster can be split (by split_l()-function for k-anonymity and l-diversity) (\$C.size >= 2k$)
                - Add (splitted) Clusters to \$SC$
                - For each cluster \$sc$ in \$SC$:
                    - Output all tuples \$t_{sc} in sc\$ with its generalization
                    - Update \$\tau$ with \$infoLoss(sc)$
                    - if \$infoLoss(sc, t_{sc})$ is smaller than \$\tau$ -> Add cluster to set of _ks-anonymized-Cluster_ _(for reuse strategy)_
                    - else delete cluster \$sc$

