# Visualizer for kafka 

Helpful to view and validate our anonymization modules. 
Offers a web-interface to plot specific keys and their distribution as well as comparing topics and therefore displaying the applied noise.

There is currently a [bug with the underlying kafka-rest-proxy](https://github.com/confluentinc/kafka-rest/issues/725), which causes the initial topic subscription to fail. It will succede on the second try. One can circumvent this issue by subscribing to two topics, e.g. input and output: now the messages from all subscribed topics can be fetched correctly.

## Install

    npm i

## Start

    npm run dev

Then go to `http://localhost:5173/` to view the visualizer web-interface.