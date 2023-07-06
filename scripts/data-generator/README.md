# Data generator

This is a module that generates data that is used to mock data streams of Ampeers Energy GmbH

The module has a following structure

```
data-generator/
├─ data/  - folder where the generated .csv goes
├─ generators/ - scripts that generate csv files
├─ config.yaml - YAML config for the producer.py script
├─ producer.py - script that produces Kafka records for chosen datasets
├─ README.md
├─ requirements.txt - Python requirements
```

## Dependencies

For this project you should have Python 3.9 installed. To install the needed packages execute the commmand in `scripts/data-generator` folder:
```
pip install -r requirements.txt
```

## Usage

To create sample data you can run any of scripts in `generators/` folder. The scripts generate data until their execution is terminated (i.e. with Ctrl+C). Data is created in the `data/` folder.

`producer.py` script can create several Kafka producers in separate processes that will simultaneously send data from several .csv files in different topics. To generate Kafka records, configure needed datasets in `config.yaml` and simply start the producer.py with `python producer.py`. More about configuration in the next section.

## Configuration <a name="config"></a>

`config.yaml` has following sections:

`bootstrap_servers` - list of one or more addresses of Kafka brokers

`dataset_topic` - dictionary for datasets to be sent in different topics. Values should be written in format "path-to-dataset": "topic-name"

`delay` - delay between two messages, sent by a single producer (valid for all created producers)

## Data Schema

Key: 'record' (constant for now)

Value: JSON dict with keys corresponding to .csv headers and values of related headers 